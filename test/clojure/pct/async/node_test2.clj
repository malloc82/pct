(ns node_test2
  (:use clojure.core pct.async.node)
  (:require [com.rpl.specter :as spr])
  (:import [java.util Arrays]))


(def test-input1 (int-array [0 0  0  0  0  0  0  0  0  0  0  0  0  0 0 0]))
(def test-exp-1   (int-array [5 9 12 14 15 15 15 15 15 15 15 15 14 12 9 5 ]))
(def test-exp-2   (let [a ^ints (Arrays/copyOf test-exp-1 (alength test-exp-1))]
                    (dotimes [i (alength a)]
                      (aset a i (* (aget a i) 2)))
                    a))

(def test-exp-5   (let [a ^ints (Arrays/copyOf test-exp-1 (alength test-exp-1))]
                    (dotimes [i (alength a)]
                      (aset a i (* (aget a i) 5)))
                    a))

(def test-input2 (let [len ^int (int 16)
                       a ^ints (int-array len)]
                   (dotimes [i len]
                     (aset a i (inc i)))
                   a))

(def test-exp-input2 (int-array [5 18 36 56 75 90 105 120 135 150 165 180 182 168 135 80]))

(def grid-16-5 (newAsyncGrid 16 5 true))


(def result1 (let [grid-16-5 (newAsyncGrid 16 5 true)]
               (test-grid-plus1 grid-16-5 test-input1 test-exp-1 unchecked-inc 1)))
(def result2 (let [grid-16-5 (newAsyncGrid 16 5 true)]
               (test-grid-plus1 grid-16-5 test-input1 test-exp-2 unchecked-inc 2)))
(def result5 (let [grid-16-5 (newAsyncGrid 16 5 true)]
               (test-grid-plus1 grid-16-5 test-input1 test-exp-5 unchecked-inc 5)))

(def result6 (let [grid-16-5 (newAsyncGrid 16 5 true)]
               ))

