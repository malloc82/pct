(ns pct.async.grid_data_test
  (:use pct.async.node )
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector]
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
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]])
  (:import [java.util HashMap TreeSet TreeMap LinkedList Iterator]
           [java.util.concurrent.locks Lock]
           [clojure.core.async.impl.channels ManyToManyChannel]
           [pct.async.node NodeConnection]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


#_(def grid-17-5 (doto (newAsyncGrid 17 5)
                 (connect-nodes)
                 (compute-offsets)))
(def grid-17-5 (newAsyncGrid 17 5 true))

(spec/valid? (spec/coll-of (spec/and :pct.async.node/all-slices-used?
                                     :pct.async.node/local-offsets-length-consistent?
                                     :pct.async.node/local-offsets-consistent?
                                     :pct.async.node/global-offset-consistent?))
             (->> (get-grid grid-17-5)
                  (spr/select grid-walker)))

;; (tap> (sort-by first > (spr/transform [spr/ALL]  (fn [x] [(count (:local-offsets x)) (:key x)]) (seq grid-17-5))))

(comment
 (defn data-copy-test [node]
   (a/go
     (let [in (:ch-in node)
           out (:ch-log node)
           local-data (alter! (iv (count (:slices node))) (fn ^long [^long _] 0))
           offset-lut (:local-offsets node)
           upstream (into #{} (keys (:local-offsets node)))]
       (loop [remaining upstream]
         (tap> (format "remaining slices: %s" remaining))
         (if (empty? remaining)
           (do (tap> (format "[%s]: all data copied" (:key node)))
               (a/>! out local-data))
           (if-let [[k v] (a/<! in)]
             (do (if (remaining k)
                   (let [[offset-v length offset-local] (k offset-lut)]
                     (copy! v local-data offset-v length offset-local)
                     (recur (disj remaining k)))
                   (recur remaining)))
             (tap> (format "[%s]: Input channel closed." (:key node)))))))))

 (data-copy-test (grid-17-5 :0-1-2-3-4))
 (a/offer! (:ch-in (grid-17-5 :0-1-2-3-4)) [:3-4-5-6 (alter! (iv 4) (fn ^long [^long _] 4))])
 (a/offer! (:ch-in (grid-17-5 :0-1-2-3-4)) [:1-2-3-4 (alter! (iv 4) (fn ^long [^long _] 2))])
 (a/offer! (:ch-in (grid-17-5 :0-1-2-3-4)) [:2-3-4-5 (alter! (iv 4) (fn ^long [^long _] 3))])
 (a/offer! (:ch-in (grid-17-5 :0-1-2-3-4)) [:0-1-2-3 (alter! (iv 4) (fn ^long [^long _] 1))])
 (a/close! (:ch-in (grid-17-5 :0-1-2-3-4)))
 )

(def seg-len (* 200 200))
(def iterations 2)

(defn grid-data-copy-test [node iterations]
  (if (= (count (:slices node)) 1)
    (a/go
      (let [local-data (alter! (dv seg-len) (fn ^double [^double _] 1.0))
            in  (:ch-in  node)
            res (:ch-log node)
            out (:ch-out node)
            key (:key node)
            offset-lut (:local-offsets node)]
        (a/>! out [key local-data])
        (tap> (format "%s sent." key))
        (loop [iter ^long (long iterations)]
          (if (> iter 0)
            (let [[k v] (a/<! in)
                  [offset-v length offset-local] (-> node :local-offsets k)]
              (tap> (format "%s received." key))
              (copy! v local-data (* offset-v seg-len) (* length seg-len) (* offset-local seg-len))
              (a/>! out [key local-data])
              (recur (unchecked-dec iter)))
            (a/>! res [key local-data])))))
    (a/go
      (let [local-data (alter! (dv (* (node) seg-len)) (fn ^double [^double _] 1.0))
            temp-v (dv (* (node) seg-len))
            in  (:ch-in  node)
            res (:ch-log node)
            out (:ch-out node)
            key (:key node)
            offset-lut (:local-offsets node)]
        (loop [iter ^long (long iterations)]
          (if (> iter 0)
            (do (loop [remaining (into #{} (keys offset-lut))]
                  (if (empty? remaining)
                    (do (axpby! 1.0 temp-v 1.0 local-data)
                        #_(alter! local-data (fn ^double [^double x] (+ x 1.0)))
                        (a/>! out [key local-data])
                        (tap> (format "%s sent." key)))
                    (if-let [[k v] (a/<! in)]
                      (if-let [[offset-v length offset-local] (-> node :local-offsets k)]
                        (do #_(copy! v local-data (* offset-v seg-len) (* length seg-len) (* offset-local seg-len))
                            (copy! v temp-v (* offset-v seg-len) (* length seg-len) (* offset-local seg-len))
                            (recur (disj remaining k)))
                        (recur remaining))
                      (tap> (format "[%s]: input channel closed.")))))
                (recur (unchecked-dec iter)))))))))


(def grid-17-5 (newAsyncGrid 17 5 true))
(distribute-all grid-17-5 grid-data-copy-test)

(def expected-17-5 [5.0 9.0 12.0 14.0 15.0 15.0 15.0 15.0 15.0 15.0 15.0 15.0 15.0 14.0 12.0 9.0 5.0])

(defn verify-vector [v seg-length expected iterations]
  (let [n (count expected)]
    (loop [i (long 0)]
      (if (< i n)
        (let [subv (subvector v (* i seg-length) seg-length)]
          (if (= (sum subv) (* (expected i) iterations seg-length))
            (recur (unchecked-inc n))
            [false i (subv 0)]))
        true))))
(time
 (def res (let [grid (newAsyncGrid 17 5 true)]
            (distribute-all grid grid-data-copy-test)
            (let [res (a/<!! (collect-data grid (fn [x] (= 1 (count (:slices x))))))
                  v (dv (* (count res) seg-len))]
              (doseq [[k [data offsets]] res]
                (let [[offset-global length offset-local] offsets]
                  (copy! data v (* offset-local seg-len) (* length seg-len) (* offset-global seg-len))))
              v))))

