(ns pct.reconstruction
  (:use clojure.core)
  (:require pct.data pct.async.node
            [clojure.core.async :as a]
            [pct.common :refer [with-out-str-data-map prime]]
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
  (:import [java.util ArrayList Iterator Arrays Collections]
           [uncomplicate.neanderthal.internal.host.buffer_block IntegerBlockVector RealBlockVector]
           ))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

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
    (transfer! x x-arr)
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
    (transfer! x x-arr)
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


(defn block-recon [^pct.async.node.AsyncNode node ^pct.data.HistoryIndex global-index ^RealBlockVector init-x ^long iterations]
  (let [{slices :slices, in :ch-in, out :ch-out, res :ch-log, key :key, offset-lut :local-offsets} node
        slice-offset (long (pct.data/slice-size* global-index))]
    (if (= (count slices) 1)
      (a/thread
        (let [[^long offset-x ^long length ^long offset-local] (:global-offset node)
              v (subvector init-x (* offset-x slice-offset) slice-offset)
              local-x (double-array (dim v))
              data-len slice-offset
              thread-name (format ">>>   Head [%15s]" key)
              [^ArrayList histories _]  (global-index (first slices) (count slices))
              h-size    ^int       (.size histories)
              step (long (prime (Math/round (* (/ h-size 12.0) 0.618))))
              shuffled-data ^objects (object-array h-size)]
          (transfer! v local-x)
          (Collections/sort histories)
          (when (< 0 h-size)
            (loop [i step
                   j (long 0)]
              (if (= i 0)
                (aset shuffled-data j (.get histories i))
                (do (aset shuffled-data j (.get histories i))
                    (recur (long (mod (+ i step) h-size))
                           (+ j 1))))))
          (timbre/info (format "%s: start: block [%d %d] %s"
                               thread-name (first slices) (count slices) [(* offset-x slice-offset) slice-offset]))
          ;; iter 0
          (let [next-x (loop [i (long 0)
                              x local-x]
                         (if (< i h-size)
                           (recur (+ i 1)
                                  (pct.data/proj_art-5* ^pct.data.PathData (aget shuffled-data i) x 0.0025))
                           x))]
            #_(a/>!! out [key (Arrays/copyOf local-x data-len)])
            (a/>!! out [key next-x])
            (timbre/info (format "%s, (%d) : data sent." thread-name 0)))
          (loop [iter (long 1)]
            (let [[k v] (a/<!! in)
                  [^long offset-v ^long length ^long offset-local] (get offset-lut k)]
              (timbre/info (format "%s, (%d), received data from %s" thread-name iter k))
              (System/arraycopy v (* offset-v slice-offset) local-x 0 length)
              (if (< iter iterations)
                (let [next-x (loop [i (long 0)
                                    x local-x]
                               (if (< i h-size)
                                 (recur (+ i 1)
                                        (pct.data/proj_art-5* ^pct.data.PathData (aget shuffled-data i) x 0.0025))
                                 x))]
                  #_(a/>!! out [key (Arrays/copyOf local-x data-len)])
                  (a/>!! out [key next-x])
                  (recur (+ iter 1)))
                (do (timbre/info (format "!!%s (%d), done. Sending out local-x" thread-name iter))
                    (a/>!! res [key [local-x (:global-offset node)]])))))))


      (a/thread
        (let [data-len    (* slice-offset (count slices))
              local-x     (double-array data-len)
              thread-name (format "--> Thread [%15s]" key)
              [^ArrayList histories _]  (global-index (first slices) (count slices))
              h-size      ^int       (.size histories)
              step  (long (prime (Math/round (* (/ h-size 12.0) 0.618))))
              shuffled-data ^objects (object-array h-size)]
          (timbre/info (format "%s started." thread-name))
          (Collections/sort histories)
          (when (< 0 h-size)
            (loop [i step
                   j (long 0)]
              (if (= i 0)
                (aset shuffled-data j (.get histories i))
                (do (aset shuffled-data j (.get histories i))
                    (recur (long (mod (+ i step) h-size))
                           (+ j 1))))))
          (loop [iter  (long 0)]
            (if (< iter iterations)
              (let [continue?
                    (boolean
                     (loop [remaining (into #{} (keys offset-lut))]
                       (if (empty? remaining)
                         (let [next-x (loop [i (long 0)
                                             x local-x]
                                        (if (< i h-size)
                                          (recur (+ i 1)
                                                 (pct.data/proj_art-5* ^pct.data.PathData (aget shuffled-data i) x 0.0025))
                                          x))]
                           (timbre/info (format "%s, (%d), sending local-x" thread-name iter))
                           (a/>!! out [key next-x])
                           true)
                         (if-let [[k v] (a/<!! in)]
                           (if-let [[^long offset-v ^long length ^long offset-local] (get offset-lut k)]
                             (do (timbre/info (format "%s, (%d), received data from %s" thread-name iter k))
                                 (System/arraycopy v (* offset-v slice-offset) local-x (* offset-local slice-offset) length)
                                 (recur (disj remaining k)))
                             (do (timbre/info (format "%s, (%d), could not find key %s, skip."
                                                      thread-name iter k))
                                 (recur remaining)))
                           (do (timbre/info (format "!!%s, (%d): incoming channel is closed. Thread is shutting down."
                                                    thread-name iter))
                               false)))))]
                (if continue?
                  (recur (+ iter 1))
                  (timbre/info (format "!!%s, iter %d: shutdown." thread-name iter))))
              (do (timbre/info (format "!!%s, finished." thread-name))))))))))



(defn async-art [^pct.async.node.AsyncGrid grid ^pct.data.HistoryIndex global-index ^RealBlockVector init-x ^long iterations]
  {:pre [(<= 0 iterations)]
   :post []}
  (pct.async.node/distribute-all grid block-recon [global-index init-x iterations])
  (let [slice-offset (long (pct.data/slice-size* global-index))
        final-x ^RealBlockVector (zero init-x)]
    #_(a/<!! (pct.async.node/collect-data grid #(= (count (:slices %)) 1)))
    (doseq [[k [data [^long global-offset ^long len ^long local-offset]]]
            (a/<!! (pct.async.node/collect-data grid #(= (count (:slices %)) 1)))]
      ;; (println k [global-offset len local-offset])
      (let [v (subvector final-x (* global-offset slice-offset) slice-offset)]
        (transfer! data v)))
    final-x))


