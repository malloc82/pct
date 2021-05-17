(ns hashmap_test
  (:use clojure.core)
  (:import [java.util HashMap]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

;; Test a performance of a small hashmap lookup vs linear lookup

(def N 4)
(def rand-keys (mapv (fn [_]
                       [(keyword (clojure.string/join "-" (repeatedly 6 #(rand-int 100)))) (rand-int 100)])
                     (range N)))


(def ^HashMap m  (HashMap. 4))

(doseq [[k v] rand-keys]
  (.put m k v))


(def key-seq (mapv (fn [_]
                     (let [i (rand-int 4)]
                       (get-in rand-keys [i 0])))
                   (range 10000)))

(time (doseq [k key-seq]
        (.get m k)))

(let [key0 (rand-keys 0)]
  (time (doseq [k key-seq]
          #_(if (= k key0) 0 1)
          (cond
            (= k ((rand-keys 0) 0)) ((rand-keys 0) 1)
            (= k ((rand-keys 1) 0)) ((rand-keys 1) 1)
            (= k ((rand-keys 2) 0)) ((rand-keys 2) 1)
            (= k ((rand-keys 3) 0)) ((rand-keys 3) 1)
            ))))

