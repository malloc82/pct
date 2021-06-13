(ns pct.data.test
  (:use clojure.core pct.common)
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [taoensso.timbre :as timbre]
            [uncomplicate.fluokitten.core :refer [fmap fmap!]]
            [uncomplicate.neanderthal
             [core :refer :all]
             [block :refer [buffer contiguous?]]
             [native :refer :all]
             [math :as math]]
            [uncomplicate.neanderthal.internal.host
             [mkl :as mkl]]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]])
  (:import [uncomplicate.neanderthal.internal.host.buffer_block RealGEMatrix RealBlockVector]))

(def ^:private target-regions {:teflon      [[122 153] 3 1.790] ;; [<col> <row>] radius expected value
                               :PMP         [[157 106] 3 0.883]
                               :LDPE        [[134  53] 3 0.979]
                               :polystyrene [[ 76  45] 3 1.024]
                               :delrin      [[ 63 146] 3 1.359]
                               :acrylic     [[ 39  92] 3 1.160]
                               :air-top     [[ 52  64] 3 0.00113]
                               :air-bottom  [[146 134] 3 0.00113]})

(def ^:private region-mask (dge [[0 0 1 1 1 0 0]
                                 [0 1 1 1 1 1 0]
                                 [1 1 1 1 1 1 1]
                                 [1 1 1 1 1 1 1]
                                 [1 1 1 1 1 1 1]
                                 [0 1 1 1 1 1 0]
                                 [0 0 1 1 1 0 0]]))


(def ^:private region-mask-arr [(int-array [0 0 1 1 1 0 0
                                            0 1 1 1 1 1 0
                                            1 1 1 1 1 1 1
                                            1 1 1 1 1 1 1
                                            1 1 1 1 1 1 1
                                            0 1 1 1 1 1 0
                                            0 0 1 1 1 0 0])
                                3 3 2]) ;; [mask-array, y-radius, x-radius, first non zero index]

(defmulti region-stat (fn [m & _] (class m)))

(defmethod region-stat (Class/forName "[D")
  [^doubles data [^long rows ^long cols] material #_[[x y] radius]]
  (assert (target-regions material) (format "Error: unknow material %s %s" material target-regions))
  (let [[[^int x ^int y] ^int radius ^int non-zero-idx] (material target-regions)
        [^ints mask-arr ^int radius_y ^int radius_x] region-mask-arr
        mask_len (alength mask-arr)
        area ^doubles (double-array mask_len)
        mask_row (inc (* radius_y 2))
        mask_col (inc (* radius_x 2))]
    (loop [r (long 0), g_offset (+ (* (- y radius_y) cols) (- x radius_x)), l_offset (long 0)]
      (if (< r mask_row)
        (do (System/arraycopy data g_offset, area l_offset, mask_col)
            (recur (unchecked-inc r) (+ g_offset cols) (+ l_offset mask_col)))
        (dotimes [i mask_len]
          (aset area i (* (aget area i) (aget mask-arr i))))))
    (let [mean (/ (reduce + area) 37.0)
          std  (double (loop [i (long 0), sum (double 0.0)]
                         (if (< i mask_len)
                           (if (= (aget mask-arr i) 0)
                             (recur (unchecked-inc i) sum)
                             (recur (unchecked-inc i) (unchecked-add sum (math/sqr (- (aget area i) mean)))))
                           (math/sqrt (/ sum 37.0)))))
          [_min _max]  (let [_m ^double (aget area non-zero-idx)]
                         (loop [i (long (inc non-zero-idx)), _min _m, _max _m]
                           (if (< i mask_len)
                             (if (= (aget mask-arr i) 0)
                               (recur (unchecked-inc i) _min _max)
                               (let [v ^double (aget area i)]
                                 (if (< v _min)
                                   (recur (unchecked-inc i) v _max)
                                   (if (< _max v)
                                     (recur (unchecked-inc i) _min v)
                                     (recur (unchecked-inc i) _min _max)))))
                             [_min _max])))]
      {:mean mean :std std :max _max :min _min})))


(defmethod region-stat RealBlockVector
  [source-vctr [^long rows ^long cols] material #_[[x y] radius]]
  (assert (target-regions material) (format "Error: unknow material %s %s" material target-regions))
  (let [[[x y] radius & _] (material target-regions)
        len  (inc (* radius 2))
        m    (trans (view-ge source-vctr rows cols))
        area (copy! (submatrix m  (- y radius)  (- x radius) len len) (ge mkl/mkl-double len len))
        _ (fmap! #(* %1 %2) area region-mask)
        mean (/ (sum area) 37.0)
        std  (math/sqrt (/ (dot (fmap #(math/sqr (- % mean)) area) region-mask) 37.0))
        v    (view-vctr area)
        max  (v (imax v))
        m    (let [_data (filterv #(not= % 0.0) v)]
               (if (empty? _data)
                 0.0
                 (apply min _data)))]
    {:mean mean :std std :max max :min m}))


(defn slice-stat [source-vctr [^long rows ^long cols] & {:keys [regions] :or {regions (keys target-regions)}}]
  (transduce (map #(vector % (region-stat source-vctr [rows cols] %)))
             (fn
               ([acc] acc)
               ([acc [k v]]
                (assoc acc k v)))
             {}
             regions))


