(ns user
  (:require pct.util.system
            pct.async.node
            pct.common
            pct.data
            pct.data.io
            [taoensso.timbre :as timbre]
            [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector]
            [clojure.tools.deps.alpha.repl :refer [add-libs]]
            [clojure.data.json :as json]
            [cheshire.core :refer :all]
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
            )
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

  (timbre/info " >>>>>>>>>>>>>>> * dev/local/user.clj loaded * <<<<<<<<<<<<<<<"  "")
  (timbre/info "    Physical Cores: " pct.util.system/PhysicalCores)
  (timbre/info "    Logical Cores:  " pct.util.system/LogicalCores)
  (timbre/info "    Max Memory:     " mem)
  (timbre/info "    JVM Max Heap:   " heap))


(def grid      (-> (pct.async.node/newAsyncGrid 16 (range 1 (+ 1 5)) :connect? true :slice-offset (* 200 200))
                   (pct.async.node/trim-connections)))
;; (def grid-9-5  (pct.async.node/newAsyncGrid 9  (range 1 (+ 1 5)) :connect? true))



#_(defn factorial ^java.math.BigInteger [^long n]
    (loop [n n
           fact ^java.math.BigInteger (java.math.BigInteger/valueOf n)]
      (if (< n 2)
        fact
        (recur (unchecked-dec n) (unchecked-multiply fact n)))))
