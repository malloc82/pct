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
            [uncomplicate.neanderthal.auxil :refer :all]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]
            [pct.common :refer [prime]]
            [pct.data.util :refer :all]
            pct.data
            pct.data.io
            pct.async.node
            [pct.reconstruction :as recon])
  (:import [pct.data.io PCTDataset]
           [java.util ArrayList HashMap TreeSet HashSet]))

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
    (def data_exp_CTP404 (with-open [dataset ^PCTDataset (pct.data.io/newPCTDataset base-dir "MLP_paths_r=1.bin" "WEPL.bin")]
                           {:x0    (.x0   dataset)
                            :rows  (.rows dataset)
                            :cols  (.cols dataset)
                            :slice-count  (.slices dataset)
                            :slice-offset (pct.data.io/slice-offset dataset)
                            :index (pct.data.io/load-dataset dataset {:min-len 75 :batch-size 80000 :style :new
                                                                      :count? false :global? false})}))
    (timbre/info (format "Path [%s] does not exist or is not a folder." base-dir))))

(def grid  (pct.async.node/newAsyncGrid 16 (range 1 (+ 1 5)) :connect? true))

(time (def result (recon/async-art grid (:index data) (:x0 data)
                                   {:iterations 6
                                    :lambda {1 0.0005
                                             2 0.0005
                                             3 0.0005
                                             4 0.0005
                                             5 0.0005}})))
