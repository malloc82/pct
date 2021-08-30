(ns pct.test
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
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]
            [pct.io :refer [load-vctr load-series save-x save-series]]
            ))


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


;; coordinates are top left pixel
;; measured on slice 7
(defonce CTP404-regions {:teflon      [[119 150] 1.790]
                         :PMP         [[154 103] 0.883]
                         :LDPE        [[132  50] 0.979]
                         :polystyrene [[ 74  42] 1.024]
                         :delrin      [[ 61 143] 1.359]
                         ;; :acrylic     [[      ]  1.160] ;; outer shell
                         :air-top     [[ 49  61] 0.00113]
                         :air-bottom  [[143 132] 0.00113]})

(def ^:private CTP404-mask (dge [[0 0 1 1 1 0 0]
                                 [0 1 1 1 1 1 0]
                                 [1 1 1 1 1 1 1]
                                 [1 1 1 1 1 1 1]
                                 [1 1 1 1 1 1 1]
                                 [0 1 1 1 1 1 0]
                                 [0 0 1 1 1 0 0]]))

;; coordinates are top left pixel
;; measured on slice 33
(defonce George-regions {:dental-enamel   [[ 35  97]  1.755]
                         :dental-dentin   [[ 55  49]  1.495]
                         :cortical-bone   [[105  35]  1.555]
                         :trabecular-bone [[147  66]  1.100]
                         :spinal-cord     [[150 118]  1.040]
                         :spinal-disc     [[111 152]  1.070]
                         :brain-tissue    [[ 60 143]  1.040]
                         :sinus           [[ 86  65]  0.220]
                         #_#_:blue-wax        [[] 0.980]})


(def ^:private George-mask (dge [[0 0 0 1 1 1 1 0 0 0]
                                 [0 0 1 1 1 1 1 1 0 0]
                                 [0 1 1 1 1 1 1 1 1 0]
                                 [1 1 1 1 1 1 1 1 1 1]
                                 [1 1 1 1 1 1 1 1 1 1]
                                 [1 1 1 1 1 1 1 1 1 1]
                                 [1 1 1 1 1 1 1 1 1 1]
                                 [0 1 1 1 1 1 1 1 1 0]
                                 [0 0 1 1 1 1 1 1 0 0]
                                 [0 0 0 1 1 1 1 0 0 0]]))



;; (def data-dir "/local/cair/dev/cells/resources/result/")
;; (def sample-set "async_run_Tuesday_9_ART_median_filter_lambda=0.001000,chunk-size=1000000,[-3,3]")
;; (def sample-x (load-vctr (s/join "/" [data-dir sample-set "x_6_9.txt"]) (fv 40000)))

#_(def sample-series (load-series (format "%s/%s/x" data-dir sample-set) :iter 6 :slices 15))

 ;; number of ones: (- 49 12) => 37

(defn region-stat [source-vctr material #_[[x y] radius] & {:keys [rows cols] :or {rows 200 cols 200}}]
  (assert (target-regions material) (format "Error: unknow material %s %s" material target-regions))
  (let [[[x y] radius & _] (material target-regions)
        len  (inc (* radius 2))
        m    (trans (view-ge source-vctr rows cols))
        area (copy! (submatrix m  (- y radius)  (- x radius) len len) (ge mkl/mkl-double len len))
        _ (fmap! #(* %1 %2) area region-mask)
        mean (/ (sum area) 37.0)
        std  (math/sqrt (/ (dot (fmap #(math/sqr (- % mean)) area) region-mask) 36.0))
        v    (view-vctr area)
        max  (v (imax v))
        m    (let [_data (filterv #(not= % 0.0) v)]
               (if (empty? _data)
                 0.0
                 (apply min _data)))]
    {:mean mean :std std :max max :min m}))


(defn slice-stat [source-vctr & {:keys [rows cols regions]
                                 :or {rows 200
                                      cols 200
                                      regions (keys target-regions)}}]
  (transduce (map #(vector % (region-stat source-vctr %)))
             (fn
               ([acc] acc)
               ([acc [k v]]
                (assoc acc k v)))
             {}
             regions))


(defn series-stat [series & {:keys [rows cols regions selection]
                             :or {rows 200
                                  cols 200
                                  regions (keys target-regions)}}]
  (let [offset (* rows cols)
        n (/ (dim series) offset)
        selection (if selection (set selection) nil)]
    (transduce (comp (filter #(if selection
                                (and (>= % 0) (< % n)
                                     (selection %))
                                true))
                     (map #(vector % (subvector series (* % offset) offset))))
               (fn
                 ([acc] acc)
                 ([acc [i v]]
                  (assoc acc i (slice-stat v :rows rows :cols cols :regions regions))))
               (sorted-map)
               (range n))))


(defn dump-slice-error-header
  "x0 is just one slice"
  [^String filename & {:keys [rows cols regions init-x]
                  :or {rows 200
                       cols 200
                       regions (keys target-regions)}}]
  (let [folder (.getParentFile (java.io.File. filename))]
    ;; (timbre/info "dump-slice-error-header: filename = " filename)
    ;; (timbre/info "dump-slice-error-header: folder = " folder)
    (when (and folder (not (.exists folder)))
      (.mkdirs folder))
    (with-open [w (clojure.java.io/writer filename :append false)]
      (.write w (format "iteration,%s\n" (s/join "," (map #(format "%s,error" (str %)) regions))))
      (.write w (format "TrueValue,%s\n" (s/join "," (map #(format "%.3f,---" (get-in target-regions [% 2])) regions))))
      (when init-x
        (let [s-stat (slice-stat init-x :rows rows :cols cols :regions regions)]
          (.write w (format "0,%s\n" (s/join "," (map #(let [m (:mean (s-stat %))
                                                             t (get-in target-regions [% 2])]
                                                         (format "%.5f,%.2f%%" m (* (/ (- m t) t) 100.0)))
                                                      regions)))))))))


(defn dump-slice-error [filename xi iter & {:keys [rows cols regions]
                                            :or {rows 200
                                                 cols 200
                                                 regions (keys target-regions)}}]
  (let [s-stat (slice-stat xi :rows rows :cols cols :regions regions)]
    (with-open [w (clojure.java.io/writer filename :append true)]
      (.write w (format "%d,%s\n" iter (s/join "," (map #(let [m (:mean (s-stat %))
                                                               t (get-in target-regions [% 2])]
                                                           (format "%.5f,%.2f%%" m (* (/ (- m t) t) 100.0)))
                                                        regions)))))))


(defn save-slice-progression [^String folder prefix slice-id & {:keys [iterations] :or {iterations 6}}]
  (if (.exists (java.io.File. folder))
    (let [region-names (keys target-regions)]
      (let-release [m (dv 40000)]
        (with-open [w (clojure.java.io/writer (format "%s/%s_%d_%d_error_progression.csv" folder prefix iterations slice-id))]
          (.write w (format "%s\n" (s/join "," (conj (map str region-names) "iteration"))))
          (.write w (format "%s\n" (s/join "," (conj (map #(get-in target-regions [% 2]) region-names) "True Value"))))
          (dotimes [i iterations]
            (let [fname (format "%s/%s_%d_%d.txt" folder prefix (inc i) slice-id)]
              (when (.exists (java.io.File. fname))
                (println "reading " fname )
                (.write w (format "%s\n" (s/join "," (conj (map #(format "%.3f" (:mean (region-stat (load-vctr fname m) %)))
                                                                region-names)
                                                           (str (inc i))))))))))))
    (throw (java.io.FileNotFoundException. (format "folder %s not found" folder)))))



#_(def m (into {} (mapv (fn [[k v]] [k (extract-region-stat sample-x v)]) insert-types)))

;; (defn show-series-region-stat [folder slice-id insert & {:keys [rows cols] :or {rows 200 cols 200}}]
;;   (transduce (filter #(.contains (.getName ^java.io.File %) (format "_%d.txt" slice-id)))
;;             (fn
;;               ([acc] acc)
;;               ([acc f]
;;                (let [name (.getName ^java.io.File f)
;;                      v (load-vctr f (fv 40000))]
;;                  (assoc acc name (:mean (pct.test/extract-region-stat v (get insert-types insert)))))))
;;             {}
;;             (file-seq (java.io.File. ^String folder))))

#_(defn show-slice-error-progress [folder slice-id insert & {:keys [rows cols] :or {rows 200 cols 200}}]
  (transduce (filter #(.contains (.getName ^java.io.File %) (format "_%d.txt" slice-id)))
            (fn
              ([acc] acc)
              ([acc f]
               (let [name (.getName ^java.io.File f)
                     v (load-vctr f (fv 40000))]
                 (assoc acc name (:mean (pct.test/extract-region-stat v (get insert-types insert)))))))
            {}
            (file-seq (java.io.File. ^String folder))))

#_(defn compute-series-stat [x inserts & {:keys [rows cols] :or {rows 200 cols 200}}]
  (let [offset (* rows cols)]
    ))


#_(def m (transduce (filter #(.contains (.getName ^java.io.File %) "_8.txt"))
            (fn
              ([acc] acc)
              ([acc f]
               (let [name (.getName ^java.io.File f)
                     v (load-vctr f (fv 40000))]
                 (assoc acc name (pct.test/extract-region-stat v (:teflon insert-types))))))
            {}
            (file-seq (java.io.File. "/local/cair/dev/cells/resources/result/async_run_Tuesday_8_DROP_median_filter_lambda=0.050000,chunk-size=1000000,[-3,3]"))))


