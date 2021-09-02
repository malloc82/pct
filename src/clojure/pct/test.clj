(ns pct.test
  (:use clojure.core)
  (:require [clojure.java.io :as io]
            [clojure.core.async :as a :refer [>!! <!! >! <!]]
            [clojure.string :as s]
            [clojure.spec.alpha :as spec]
            [taoensso.timbre :as timbre]
            [uncomplicate.fluokitten.core :refer [fmap fmap!]]
            [uncomplicate.neanderthal
             [core :refer :all]
             [block :refer [buffer contiguous?]]
             [native :refer :all]
             [math :as math]]
            [uncomplicate.neanderthal.internal.host
             [mkl :as mkl]]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]
            [pct.data.io :refer [load-vctr load-series #_#_save-x save-series]]
            )
  (:import [uncomplicate.neanderthal.internal.host.buffer_block IntegerBlockVector RealBlockVector RealGEMatrix]))


;; From Paniz

;; centersx =  newArray( 146.5, 52.5, 157.5,  134.5,  76.5, 39.5, 63.5, 122.5);
;; centersy = newArray( 134.5, 64.5, 106.5,   53.5,   45.5, 92.5, 146.5, 153.5);
;; insert_names = newArray("Air (bottom)", "Air (top)", "PMP", "LDPE", "Polystyrene", "Acrylic", "Delrin", "Teflon");

;; format [[x, y], radius, true value]
(defonce target-regions {:teflon      [[122 153] 3 1.790]
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

(defprotocol IRegionStats
  (mean*  [this slice])
  (std*   [this slice])
  (error* [this slice]))

(defprotocol IRegion
  (mask-filter* [this slice]))


(defrecord RecRegion [^long x ^long y ^long cols ^long rows ^long n ^RealGEMatrix mask ^double expected]
  IRegion
  (mask-filter* [this slice]
    (fmap #(* ^double %1 (double %2)) (submatrix slice x y cols rows) mask))

  IRegionStats
  (mean* [this slice]
    (/ ^double (sum (mask-filter* this slice)) n))

  (std* [this slice]
    (let [area  (mask-filter* this slice)
          _mean (/ ^double (sum area) n)]
      (math/sqrt (/ ^double (dot (fmap #(math/sqr (- ^double % _mean)) area) mask) n))))

  (error* [this slice]
    (let [^double _mean (mean* this slice)]
      (* (/ (- _mean expected) expected) 100))))



(defn newRecRegion [^long x ^long y ^RealGEMatrix mask ^double expected]
  (->RecRegion x y (ncols mask) (mrows mask) (sum mask) mask expected))

;; coordinates are top left pixel
;; measured on slice 7

(def CTP404-mask (trans (dge [[0 0 1 1 1 0 0]
                              [0 1 1 1 1 1 0]
                              [1 1 1 1 1 1 1]
                              [1 1 1 1 1 1 1]
                              [1 1 1 1 1 1 1]
                              [0 1 1 1 1 1 0]
                              [0 0 1 1 1 0 0]])))


(def acrylic-vertical-mask (trans (dge [[0 0 1 0 0]
                                        [0 1 1 1 0]
                                        [0 1 1 1 0]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [1 1 1 1 1]
                                        [0 1 1 1 0]
                                        [0 1 1 1 0]
                                        [0 0 1 0 0]])))


(def acrylic-horizontal-mask (trans (dge [[0 0 0 1 1 1 1 1 1 1 1 1 1 1 1 0 0 0]
                                          [0 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 0]
                                          [1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1]
                                          [0 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 0]
                                          [0 0 0 1 1 1 1 1 1 1 1 1 1 1 1 0 0 0]])))


(def George-mask (trans (dge [[0 0 0 1 1 1 1 0 0 0]
                              [0 0 1 1 1 1 1 1 0 0]
                              [0 1 1 1 1 1 1 1 1 0]
                              [1 1 1 1 1 1 1 1 1 1]
                              [1 1 1 1 1 1 1 1 1 1]
                              [1 1 1 1 1 1 1 1 1 1]
                              [1 1 1 1 1 1 1 1 1 1]
                              [0 1 1 1 1 1 1 1 1 0]
                              [0 0 1 1 1 1 1 1 0 0]
                              [0 0 0 1 1 1 1 0 0 0]])))


(def CTP404-regions {:teflon         (newRecRegion 119 150 CTP404-mask 1.790)
                     :PMP            (newRecRegion 154 103 CTP404-mask 0.883)
                     :LDPE           (newRecRegion 132  50 CTP404-mask 0.979)
                     :polystyrene    (newRecRegion  74  42 CTP404-mask 1.024)
                     :delrin         (newRecRegion  61 143 CTP404-mask 1.359)
                     :air-top        (newRecRegion  49  61 CTP404-mask 0.00113)
                     :air-bottom     (newRecRegion 143 132 CTP404-mask 0.00113)
                     :acrylic-left   (newRecRegion  26  91 acrylic-vertical-mask 1.160)
                     :acrylic-right  (newRecRegion 169  91 acrylic-vertical-mask 1.160)
                     :acrylic-top    (newRecRegion  91  26 acrylic-horizontal-mask 1.160)
                     :acrylic-bottom (newRecRegion  91 169 acrylic-horizontal-mask 1.160)})

(def George-regions {:dental-enamel   (newRecRegion  35  97 George-mask 1.495) ;; 1.755
                     :dental-dentin   (newRecRegion  55  49 George-mask 1.755) ;; 1.495
                     :cortical-bone   (newRecRegion 105  35 George-mask 1.555)
                     :trabecular-bone (newRecRegion 147  66 George-mask 1.100)
                     :spinal-cord     (newRecRegion 150 118 George-mask 1.040)
                     :spinal-disc     (newRecRegion 111 152 George-mask 1.070)
                     :brain-tissue    (newRecRegion  60 143 George-mask 1.040)
                     :sinus           (newRecRegion  86  65 George-mask 0.220)
                     #_#_:blue-wax        [[] 0.980]})

(spec/def ::regions-spec (spec/map-of keyword? #(satisfies? IRegionStats %)))

(comment
  ;; old stuffs
  (let [n    (sum CTP404-mask)
        rows (mrows George-mask)
        cols (ncols George-mask)]
    (def CTP404-regions {:teflon       [[119 150] [cols rows] n CTP404-mask 1.790]
                         :PMP          [[154 103] [cols rows] n CTP404-mask 0.883]
                         :LDPE         [[132  50] [cols rows] n CTP404-mask 0.979]
                         :polystyrene  [[ 74  42] [cols rows] n CTP404-mask 1.024]
                         :delrin       [[ 61 143] [cols rows] n CTP404-mask 1.359]
                         :air-top      [[ 49  61] [cols rows] n CTP404-mask 0.00113]
                         :air-bottom   [[143 132] [cols rows] n CTP404-mask 0.00113]
                         #_#_:acrylic-left [[ 26  90]  1.160] ;; outer shell
                         }))



  ;; coordinates are top left pixel
  ;; measured on slice 33

  (let [n    (sum George-mask)
        rows (mrows George-mask)
        cols (ncols George-mask)]
    (defonce George-regions {:dental-enamel   [[ 35  97]  [cols rows] n George-mask 1.755]
                             :dental-dentin   [[ 55  49]  [cols rows] n George-mask 1.495]
                             :cortical-bone   [[105  35]  [cols rows] n George-mask 1.555]
                             :trabecular-bone [[147  66]  [cols rows] n George-mask 1.100]
                             :spinal-cord     [[150 118]  [cols rows] n George-mask 1.040]
                             :spinal-disc     [[111 152]  [cols rows] n George-mask 1.070]
                             :brain-tissue    [[ 60 143]  [cols rows] n George-mask 1.040]
                             :sinus           [[ 86  65]  [cols rows] n George-mask 0.220]
                             #_#_:blue-wax        [[] 0.980]})))


(defn region-stats
  [^RealGEMatrix slice ^RecRegion region]
  (let [^double _mean (mean* region slice)]
    {#_#_:n     (:n region)
     :mean  _mean
     :std   (std* region slice)
     :error (let [^double rsp (:expected region)]
              (* (/ (- _mean rsp) rsp) 100))}))

(defn region-stats-stacked
  [slices ^RecRegion region]
  (let [n     (count slices)
        _mean (/ ^double (reduce + (mapv #(mean* region %) slices)) n)
        mask  (:mask region)
        areas (mapv #(mask-filter* region %) slices)]
    {#_#_:n (* ^long (:n region) n)
     :mean _mean
     :std (math/sqrt (/ ^double (reduce + (mapv (fn [area] (dot (fmap #(math/sqr (- ^double % _mean)) area) mask)) areas))
                        n))
     :error (let [^double rsp (:expected region)]
              (* (/ (- _mean rsp) rsp) 100))}))


(defn slice-stats
  [^RealGEMatrix slice regions]
  {:pre [(spec/valid? ::regions-spec regions)]}
  (into {} (mapv (fn [[k v]]
                   ;; (println v)
                   [k (region-stats slice v)])
                 regions)))


(defn series-stats
  [^RealBlockVector x [^long rows ^long cols ^long slices] regions sample-idx]
  {:pre [(spec/valid? ::regions-spec regions)
         (spec/valid? (spec/coll-of (and int? #(< ^long % slices))) sample-idx)]}
  (let [slice-offset (* rows cols)]
    (let [slices (mapv (fn [^long i]
                         (trans (view-ge (subvector x (* i slice-offset) slice-offset)
                                         rows cols)))
                       sample-idx)]
      (into {} (mapv (fn [[k v]] [k (region-stats-stacked slices v)]) regions)))))


(defn compare-stats
  ([stats-1 stats-2]
   (compare-stats stats-1 stats-2 [:all]))
  ([stats-1 stats-2 attr]
   (let [curr-attr (first attr)
         rest-attr (or (next  attr) [:all])]
     ;; (println curr-attr rest-attr)
     (into {} (mapv (fn [[k v1]]
                      (if (or (identical? curr-attr :all)
                              (identical? curr-attr k))
                        (if-let [v2 (stats-2 k)]
                          (if (instance? java.util.Map v1)
                            [k (compare-stats v1 v2 rest-attr)]
                            [k [v1 v2]])
                          [k [v1 nil]])))
                    stats-1)))))


