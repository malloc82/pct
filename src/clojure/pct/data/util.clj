(ns pct.data.util
  (:use clojure.core)
  (:require pct.data)
  (:import [java.util ArrayList]))


(defn head
  ([g]
   (head g 10))
  ([^ArrayList g ^long n]
   (let [len (.size  g)]
     (for [i (range (min len n))]
       (.get g i)))))

(defn tail
  ([g]
   (tail g 10))
  ([^ArrayList g ^long n]
   (let [len (.size  g)]
     (for [i (range (- len n) len)]
       (.get g i)))))

(defn body [^ArrayList g ^long start ^long end]
  (let [len (.size g)]
     (for [i (range (min start len) (min end len))]
       (.get g i))))

(defn order-by [^ArrayList data method]
  (case method
    :id
    (do (java.util.Collections/sort
         data
         (fn ^long [a b]
           (cond
             (> ^int (.id  ^pct.data.PathData a) ^int (.id  ^pct.data.PathData b)) (int  1)
             (< ^int (.id  ^pct.data.PathData a) ^int (.id  ^pct.data.PathData b)) (int -1)
             (= ^int (.id  ^pct.data.PathData a) ^int (.id  ^pct.data.PathData b)) (int  0))))
        true)
    :entry-xy
    (do (java.util.Collections/sort
         data
         (fn ^long [a b]
           (cond
             (> ^float (.entry-xy  ^pct.data.PathData a) ^float (.entry-xy  ^pct.data.PathData b)) (int  1)
             (< ^float (.entry-xy  ^pct.data.PathData a) ^float (.entry-xy  ^pct.data.PathData b)) (int -1)
             (= ^float (.entry-xy  ^pct.data.PathData a) ^float (.entry-xy  ^pct.data.PathData b)) (int  0))))
        true)

    :entry-xz
    (do (java.util.Collections/sort
         data
         (fn ^long [a b]
           (cond
             (> ^float (.entry-xz  ^pct.data.PathData a) ^float (.entry-xz  ^pct.data.PathData b)) (int  1)
             (< ^float (.entry-xz  ^pct.data.PathData a) ^float (.entry-xz  ^pct.data.PathData b)) (int -1)
             (= ^float (.entry-xz  ^pct.data.PathData a) ^float (.entry-xz  ^pct.data.PathData b)) (int  0))))
        true)

    false))



