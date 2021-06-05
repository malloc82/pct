(ns pct.vector_test
  (:use clojure.core)
  (:require [pct.data.vectors :refer :all]
            [clojure.pprint :refer [cl-format]]))

(deftype abc [^int a])

(def v (double-vector 10))

(dotimes [i (dim v)])

