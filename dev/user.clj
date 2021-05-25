(ns user
  (:require pct.util.system
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
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]))

;; (set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(println "\nHello, user. Welcome to" (str (pct.util.system/localhost)))
(println "    Physical Cores: " pct.util.system/PhysicalCores)
(println "    Logical Cores:  " pct.util.system/LogicalCores)
(println "    Max Memory:     " (let [[s u] (pct.util.system/readableFormat pct.util.system/MaxMemory)]
                                  (format "%.2f %s" s u)))
(println "    JVM Max Heap:   " (let [[s u] (pct.util.system/readableFormat pct.util.system/MaxHeapSize)]
                                  (format "%.2f %s" s u)))
(println "")

