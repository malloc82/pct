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
            pct.data.test
            pct.async.node
            [pct.reconstruction :as recon])
  (:import [java.util ArrayList HashMap TreeSet HashSet]
           [java.text DecimalFormat]))

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


(let [base-dir "datasets/george/02272019-Run23-interpolated/test/output-1mm/B_1280000_L_1.000000"
      f (clojure.java.io/file base-dir)]
  (if (.isDirectory f)
    (def data_george_1mm_test (with-open [dataset ^pct.data.io.PCTDataset (pct.data.io/newPCTDataset
                                                                           {:rows   200
                                                                            :cols   200
                                                                            :slices 80
                                                                            :dir    base-dir
                                                                            :path   "MLP_paths_r=0.bin"
                                                                            :fmt :new})]
                                {:x0    (.x0   dataset)
                                 :rows  (.rows dataset)
                                 :cols  (.cols dataset)
                                 :slice-count  (.slices dataset)
                                 :slice-offset (pct.data.io/slice-offset dataset)
                                 :index (pct.data.io/load-dataset dataset {:min-len 75 :batch-size 80000 :style :new
                                                                           :count? false :global? false})}))
    (timbre/info (format "Path [%s] does not exist or is not a folder." base-dir))))


(let [base-dir "datasets/data_4_Ritchie/exp_CTP404/10_24_2019"
      f (clojure.java.io/file base-dir)]
  (if (.isDirectory f)
    (def data_exp_CTP404 (with-open [dataset ^pct.data.io.PCTDataset (pct.data.io/newPCTDataset
                                                                      {:rows   200
                                                                       :cols   200
                                                                       :slices 16
                                                                       :dir    base-dir
                                                                       :path   "MLP_paths_r=1.bin"
                                                                       :b      "WEPL.bin"
                                                                       :fmt    :old})]
                           {:x0    (.x0   dataset)
                            :rows  (.rows dataset)
                            :cols  (.cols dataset)
                            :slice-count  (.slices dataset)
                            :slice-offset (pct.data.io/slice-offset dataset)
                            :index (pct.data.io/load-dataset dataset {:min-len 75 :batch-size 80000 :style :new
                                                                      :count? false :global? false})}))
    (timbre/info (format "Path [%s] does not exist or is not a folder." base-dir))))
