(ns user
  (:use clojure.core)
  (:require pct.util.system
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
             [block :refer [buffer contiguous?]]
             [native :refer :all]]
            [uncomplicate.neanderthal.auxil :refer :all]
            [uncomplicate.neanderthal.internal
             [api :as api]]
            [uncomplicate.neanderthal.internal.host
             [mkl :as mkl]]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]
            [pct.common :refer [prime]]
            [pct.data.util :refer :all]
            pct.data
            pct.data.io
            pct.async.node
            [pct.reconstruction :as recon])
  (:import [java.util ArrayList HashMap]))

;; (set! *warn-on-reflection* true)
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
    (def data (with-open [dataset (pct.data.io/newPCTDataset
                                   {:rows   200
                                    :cols   200
                                    :slices 16
                                    :dir    "datasets/data_4_Ritchie/exp_CTP404/10_24_2019"
                                    :path   "MLP_paths_r=1.bin"
                                    :b      "WEPL.bin"})]
                {:index (pct.data.io/load-dataset dataset {:min-len 0 :batch-size 80000 :style :new
                                                           :count? true :global? false})
                 :x0 (.x0 dataset)}))
    (timbre/info (format "Path [%s] does not exist or is not a folder." base-dir))))

(def grid (pct.async.node/newAsyncGrid 16 5 true))
(time (def image (recon/async-art grid (:index data) (:x0 data) 6)))


;; (def maps [{:a "Example1" :b {:c "Example2" :id 1}}
;;  	       {:a "Example3" :b {:c "Example4" :id 2}}
;; 	       {:a "Example5" :b {:c "Example6" :id 3}}])

;; (spr/select [spr/ALL (fn [x] (= (-> x :b :id) 1))] maps)
;; (spr/transform [spr/ALL spr/MAP-VALS #(:c %)]
;;                (fn [x] (if (= (:id x) 2)
;;                         (assoc x :c "new str")
;;                         x)) maps)
