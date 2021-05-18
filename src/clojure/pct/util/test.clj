(ns pct.util.test
  (:use clojure.core pct.async.node)
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector]
            [com.rpl.specter :as spr]
            [uncomplicate.commons.core :refer [with-release]]
            [uncomplicate.neanderthal
             [core :refer :all]
             [block :refer [buffer contiguous?]]
             [native :refer :all]]
            [uncomplicate.neanderthal.auxil :refer :all]
            [uncomplicate.neanderthal.internal
             [api :as api]]
            [uncomplicate.neanderthal.internal.host
             [mkl :as mkl]]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]])
  (:import [java.util HashMap TreeSet TreeMap LinkedList Iterator]
           [java.util.concurrent.locks Lock]
           [clojure.core.async.impl.channels ManyToManyChannel]
           ;; [pct.async.node NodeConnection]
           ))

(defn hello
  ([] (println "Hi there."))
  ([s] (println (format "Hello, %s" (:stuff s)))))


(defn load-test [& args]
  (println "")
  (println "args: " args)
  (println "")
  (println "*******************************************")
  (println "*                                         *")
  (println "*    Everything's loaded successfully.    *")
  (println "*                                         *")
  (println "*******************************************")
  (println "")

  (System/exit 0))
