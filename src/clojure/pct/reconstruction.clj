(ns pct.reconstruction
  (:use clojure.core)
  (:require pct.data
            [pct.common :refer [with-out-str-data-map]]
            [taoensso.timbre :as timbre]
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
  (:import [java.util ArrayList Iterator]
   [uncomplicate.neanderthal.internal.host.buffer_block IntegerBlockVector RealBlockVector]
           ))


(defn art-test-iteration
  "Vanilla version of POCS, test iteration speed bwtween index vs iterator"
  ^RealBlockVector [^ArrayList histories ^RealBlockVector x ^double lambda]
  [(with-out-str-data-map
     (time
      (let [len (.size histories)]
        (loop [i (long 0)
               acc (double 0.0)]
          (if (< i len)
            (let [h ^pct.data.PathData (.get histories i)]
              (recur (unchecked-inc i) (+ (.chord-len h) acc)))
            acc)))))
   (with-out-str-data-map
     (time
      (let [it (clojure.lang.RT/iter histories)]
        (loop [acc (double 0.0)]
          (if (.hasNext it)
            (let [h ^pct.data.PathData (.next it)]
              (recur (+ (.chord-len h) acc)))
            acc)))))])


(defn art-test1
  "Vanilla version of POCS"
   [^ArrayList histories ^RealBlockVector x ^double lambda]
  (let [x-arr ^double (double-array (dim x))
        x0 (copy x)
        x1 (copy x)
        x2 (copy x)]
    (-> (.buffer x)
        .asDoubleBuffer
        (.get x-arr))
    [(with-out-str-data-map
       (time
        (let [len (.size histories)]
          (loop [i (long 0)
                 x x0]
            (if (< i len)
              (recur (unchecked-inc i)
                     (pct.data/proj_art-1* ^pct.data.PathData (.get histories i) x lambda))
              x)))))
     (with-out-str-data-map
       (time
        (let [len (.size histories)]
          (loop [i (long 0)
                 x x1]
            (if (< i len)
              (recur (unchecked-inc i)
                     (pct.data/proj_art-2* ^pct.data.PathData (.get histories i) x lambda))
              x)))))
     (with-out-str-data-map
       (time
        (let [len (.size histories)]
          (loop [i (long 0)
                 x x2]
            (if (< i len)
              (recur (unchecked-inc i)
                     (pct.data/proj_art-3* ^pct.data.PathData (.get histories i) x lambda))
              x)))))
     (with-out-str-data-map
         (time
          (let [len (.size histories)]
            (loop [i (long 0)
                   x x-arr]
              (if (< i len)
                (recur (unchecked-inc i)
                       (pct.data/proj_art-4* ^pct.data.PathData (.get histories i) x lambda))
                x)))))
     #_(with-out-str-data-map
         (time
          (let [len (.size histories)]
            (loop [i (long 0)]
              (if (< i len)
                (let [h ^pct.data.PathData (.get histories i)
                      path ^ints (.path h)
                      n ^int (alength path)
                      a ^double (* lambda (/ (- (.energy h) ^double (pct.data/dot* h x))
                                             (* n (.chord-len h))))]
                  (loop [k (int 0)]
                    (if (< k n)
                      (let [i ^int (aget path k)
                            xi (double (x i))]
                        (if (not= xi 0.0)
                          (x i (+ xi a)))
                        (recur (unchecked-inc-int k)))))
                  (recur (unchecked-inc i)))
                x)))))]
    ))

(defn art-test2
  ^doubles [^ArrayList histories ^RealBlockVector x ^double lambda]
  (let [x-arr ^double (double-array (dim x))]
    (-> (.buffer x)
        .asDoubleBuffer
        (.get x-arr))
    (let [len (.size histories)]
      (println len)
      (loop [i (long 0)
             x x-arr]
        (if (< i len)
          (recur (unchecked-inc i)
                 (pct.data/proj_art-4* ^pct.data.PathData (.get histories i) x lambda))
          x)))))


#_(let [n (* 200 200 16)
      a ^doubles (double-array n)
      v ^RealBlockVector (dv n)]
  [(with-out-str-data-map
     (time
      (loop [i (long 0)
             x (double 0.0)]
        (if (< i n)
          (recur (unchecked-inc i) (+ x (aget a (rand-int n))))
          x))))
   (with-out-str-data-map
     (time
      (loop [i (long 0)
             x (double 0.0)]
        (if (< i n)
          (recur (unchecked-inc i) (+ x (v (rand-int n))))
          x))))])
