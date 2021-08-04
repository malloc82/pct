(ns pct.tvs
  (:use clojure.core)
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a :refer [>!! <!! >! <!]]
            [clojure.string :as s]
            [taoensso.timbre :as timbre]
            [uncomplicate.fluokitten.core :refer [fmap fmap!]]
            [uncomplicate.neanderthal
             [core :refer :all]
             [block :refer [buffer contiguous?]]
             [native :refer :all]
             [math :as math]]
            [uncomplicate.neanderthal.internal.host
             [mkl :as mkl]]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]))

(set! *warn-on-reflection* true)
;; (set! *unchecked-math* true)
(set! *unchecked-math* :warn-on-boxed)




(defn tv [data [^long rows ^long cols ^long slices] & {:keys [v]}]
  (let [v        (or v (zero data))
        offset   (* rows cols)
        last-idx (dec (dim data))
        last-row (dec rows)
        last-col (dec cols)
        norm_G ^double (loop [i (int 0)
                              sum_g_norm 0.0]
                         (if (< i last-idx)
                           (let [slice_offset (rem i offset)
                                 r (quot slice_offset cols)
                                 c (rem  slice_offset cols)]
                             (if (and (< r last-row) (< c last-col))
                               (let [data_i   (double (data i))
                                     g_x_h    (- ^double (data (+ i 1))    data_i)
                                     g_y_h    (- ^double (data (+ i cols)) data_i)
                                     g_norm_h (math/sqrt (+ (math/sqr g_y_h) (math/sqr g_x_h)))]
                                 (when (> g_norm_h 0.0)
                                   (let [i+1    (+ i 1)
                                         i+cols (+ i cols)]
                                     (v i      (- ^double (v i)      (/ (+ g_x_h  g_y_h) g_norm_h)))
                                     (v i+1    (+ ^double (v i+1)    (/ g_x_h g_norm_h)))
                                     (v i+cols (+ ^double (v i+cols) (/ g_y_h g_norm_h)))))
                                 (recur (inc i) (+ sum_g_norm g_norm_h)))
                               (recur (inc i) sum_g_norm)))
                           (nrm2 v)))]
    (if (= norm_G 0.0)
      (let [z (zero v)]
        (release v)
        z)
      (scal! (/ 1.0 ^double norm_G) v))))


(defn tv2 ^doubles [^doubles data [^long rows ^long cols]]
  (let [data-len (alength data)
        v        (double-array data-len)
        offset   (* rows cols)
        last-idx (dec data-len)
        last-row (dec rows)
        last-col (dec cols)
        norm_G   (loop [i (int 0)
                        sum_g_norm 0.0]
                   (if (< i last-idx)
                     (let [slice_offset (rem i offset)
                           r (quot slice_offset cols)
                           c (rem  slice_offset cols)]
                       (if (and (< r last-row) (< c last-col))
                         (let [data_i   (aget data i)
                               g_x_h    (- ^double (aget data (+ i 1))    data_i)
                               g_y_h    (- ^double (aget data (+ i cols)) data_i)
                               g_norm_h (math/sqrt (+ (math/sqr g_y_h) (math/sqr g_x_h)))]
                           (when (> g_norm_h 0.0)
                             (let [i+1    (+ i 1)
                                   i+cols (+ i cols)]
                               (aset v i      (- ^double (aget v i)      (/ (+ g_x_h  g_y_h) g_norm_h)))
                               (aset v i+1    (+ ^double (aget v i+1)    (/ g_x_h g_norm_h)))
                               (aset v i+cols (+ ^double (aget v i+cols) (/ g_y_h g_norm_h)))))
                           (recur (inc i) (+ sum_g_norm g_norm_h)))
                         (recur (inc i) sum_g_norm)))
                     #_(nrm2 v)
                     ;; calculate 2norm of v
                     (loop [i (long 1)
                            sum (let [a (aget v 0)]
                                  (* a a))]
                       (if (< i data-len)
                         (recur (unchecked-inc i) (+ sum (let [a (aget v i)]
                                                           (* a a))))
                         (Math/sqrt sum)))))]
    (if (= ^double norm_G 0.0)
      (double-array data-len)
      (loop [i (long 0)]
        (if (< i data-len)
          (do (aset v i (/ (aget v i) ^double norm_G))
              (recur (unchecked-inc i)))
          v)))))


;; alpha could be 0.75 or 0.05
(defn ntvs [x dim ell
            & {:keys [alpha N in-place]
               :or {alpha 0.75 N 5 in-place false}}]
  (let [last-ell (+ ^long ell ^long N)]
    (loop [ell ^long ell
           x (if in-place x (copy x))]
      (if (< ell last-ell)
        (let [v (tv x dim)
              x_n+1 (axpy! (- (Math/pow ^double alpha ^long ell)) v x)]
          (println ell)
          (recur (unchecked-inc ell) x_n+1))
        x))))


;; alpha could be 0.75 or 0.05
(defn ntvs-slice
  [^doubles x dim ell
   & {:keys [alpha N in-place]
      :or {alpha 0.75 N 5 in-place false}}]
  (let [len (alength x)
        last-ell (+ ^long ell ^long N)
        x ^doubles (if in-place x (java.util.Arrays/copyOf x len))]
    (loop [ell ^long ell]
      (if (< ell last-ell)
        (let [v ^doubles (tv2 x dim)
              a (Math/pow ^double alpha ^long ell)]
          #_(println ell)
          (dotimes [i len]
            (aset x i (- (aget x i) (* a (aget v i)))))
          (recur (unchecked-inc ell)))
        x))))


(defn ntvs-ell-seq [^long K ^long N]
  (let [ell 0]
    (loop [k 0
           ell 0]
      (if (< k K)
        (let [ell_next (+ (long (min ell k)) (long (rand-int (Math/abs (- ell k)))))]
          (println (format "iteration %2d: rand(%2d, %2d)  -> %2d" k k ell ell_next))
          (recur (inc k) (+ ell_next N)))))))


