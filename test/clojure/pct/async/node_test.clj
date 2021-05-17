(ns pct.async.node_test
  (:use pct.async.node)
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector])
  (:import [java.util HashMap TreeSet TreeMap LinkedList Iterator]
           [java.util.concurrent.locks Lock]
           [clojure.core.async.impl.channels ManyToManyChannel]
           [pct.async.node NodeConnection]))


(def grid-4 (newAsyncGrid 4 3))
(connect-nodes grid-4)
(spec/valid? :pct.async.node/all-slices-used? grid-4)
(compute-index-offsets grid-4)
(tap> (transform [grid-walker] (fn [x] [(:slices x) (.connection ^NodeConnection (:upstream x))])(:grid grid-4)))
(count (select grid-walker (:grid grid-4)))


