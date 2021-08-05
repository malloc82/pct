(ns user
  (:use clojure.core)
  (:require pct.util.system
            [pct.data.test  :refer :all]
            [pct.data.image :refer :all]
            [taoensso.timbre :as timbre]
            [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector]
            [clojure.tools.deps.alpha.repl :refer [add-libs]]
            [com.rpl.specter :as spr]
            [uncomplicate.neanderthal
             [core :refer :all]
             [native :refer :all]]
            ;; [uncomplicate.neanderthal.auxil :refer :all]
            ;; [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]
            [pct.common :refer [prime]]
            [pct.data.util :refer :all]
            pct.data
            pct.data.io
            pct.async.node
            [pct.reconstruction :as recon])
  (:import [java.util ArrayList HashMap TreeSet HashSet]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(let [mem (let [[s u] (pct.util.system/readableFormat pct.util.system/MaxMemory)]
            (format "%.2f %s" s u))
      heap (let [[s u] (pct.util.system/readableFormat pct.util.system/MaxHeapSize)]
             (format "%.2f %s" s u))]
  (println "\nHello, user. Welcome to" (str (pct.util.system/localhost)))
  (println "    Physical Cores: " pct.util.system/PhysicalCores)
  (println "    Logical Cores:  " pct.util.system/LogicalCores)
  (println "    Max Memory:     " mem)
  (println "    JVM Max Heap:   " heap)
  (println "")

  (timbre/info " >>>>>>>>>>>>>>> * dev/lm001/user.clj loaded * <<<<<<<<<<<<<<<"  "")
  (timbre/info "    Physical Cores: " pct.util.system/PhysicalCores)
  (timbre/info "    Logical Cores:  " pct.util.system/LogicalCores)
  (timbre/info "    Max Memory:     " mem)
  (timbre/info "    JVM Max Heap:   " heap))


(let [base-dir "datasets/data_4_Ritchie/exp_CTP404/10_24_2019"
      f (clojure.java.io/file base-dir)]
  (if (.isDirectory f)
    (def data (with-open [dataset ^pct.data.io.PCTDataset (pct.data.io/newPCTDataset
                                                           {:rows   200
                                                            :cols   200
                                                            :slices 16
                                                            :dir    "datasets/data_4_Ritchie/exp_CTP404/10_24_2019"
                                                            :path   "MLP_paths_r=1.bin"
                                                            :b      "WEPL.bin"})]
                {:x0    (.x0 dataset)
                 :index (pct.data.io/load-dataset dataset {:min-len 50 :batch-size 80000 :style :new
                                                           :count? false :global? false})}))
    (timbre/info (format "Path [%s] does not exist or is not a folder." base-dir))))

#_(def grid (pct.async.node/newAsyncGrid 16 5 true))
(def grid  (pct.async.node/newAsyncGrid 16 (range 1 (+ 1 5)) :connect? true))
;; (def grid2  (pct.async.node/newAsyncGrid 16 (range 2 (+ 2 1)) :connect? true))
;; (def grid3  (pct.async.node/newAsyncGrid 15 (range 2 (+ 2 1)) :connect? true))
;; (def grid4  (pct.async.node/newAsyncGrid 17 (range 3 (+ 3 2)) :connect? true))

(time (def result (recon/async-art grid (:index data) (:x0 data)
                                   {:tvs? true
                                    :iterations 6
                                    :lambda {1 0.0005
                                             2 0.0005
                                             3 0.0005
                                             4 0.0005
                                             5 0.0005}})))



;; (def slice_6 (subvector image (* 6 200 200) (* 200 200)))

;; (def slice_6_arr (double-array (* 200 200)))
;; (transfer! slice_6 slice_6_arr)

;; (imshow slice_6_arr [200 200])

;; (def slice_6_stat (test/slice-stat slice_6 [200 200]))


;; (def maps [{:a "Example1" :b {:c "Example2" :id 1}}
;;  	       {:a "Example3" :b {:c "Example4" :id 2}}
;; 	       {:a "Example5" :b {:c "Example6" :id 3}}])

;; (spr/select [spr/ALL (fn [x] (= (-> x :b :id) 1))] maps)
;; (spr/transform [spr/ALL spr/MAP-VALS #(:c %)]
;;                (fn [x] (if (= (:id x) 2)
;;                         (assoc x :c "new str")
;;                         x)) maps)


#_(time (def res (let [n (count (first ((:index data) 4 2)))
                step ^long (prime (int (* (/ n 12) 0.382)))
                s1 (HashSet. (range n))
                s2 (HashSet.)]
            (loop [i (long step)
                   j (long 0)]
              (if (= i 0)
                (do (.add s2 0)
                    [(.equals ^HashSet s2 ^HashSet s1) s1 s2])
                (do (.add ^HashSet s2 i)
                    (recur (mod (+ i step) n)
                           (unchecked-inc j))))))))


;; (def _rows 15)
;; (def _cols 15)
;; (def radius 2)

;; (loop [r radius
;;        top-left 0
;;        idx (+ (* r _cols) radius)]
;;   (when (< r (- _rows radius))
;;     (loop [c radius
;;            top-left top-left
;;            idx idx]
;;       (when (< c (- _cols radius))
;;         (print [top-left (+ (* r _cols) c) idx])
;;         (print " ")
;;         (recur (inc c) (inc top-left) (inc idx))))
;;     (println "")
;;     (recur (inc r) (+ top-left _cols) (+ idx _cols))))

