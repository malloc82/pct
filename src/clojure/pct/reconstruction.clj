(ns pct.reconstruction
  (:use clojure.core)
  (:require pct.data
            pct.tvs
            [pct.async.node :as async-node :refer [recv-upstream send-downstream]]
            [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [pct.common :refer [with-out-str-data-map]]
            [pct.util.prime :as prime]
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
  (:import [java.util ArrayList Iterator Arrays Collections HashMap]
           [uncomplicate.neanderthal.internal.host.buffer_block IntegerBlockVector RealBlockVector]
           [java.text DecimalFormat]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^{:private true :tag 'DecimalFormat}    float-format (DecimalFormat. "#.########"))
(def ^{:private true :tag 'java.lang.String} _normal_  "  ")
(def ^{:private true :tag 'java.lang.String} _warning_ " !")
(def ^{:private true :tag 'java.lang.String} _term_    "!!")

(defn art-test-iteration
  "Vanilla version of POCS, test iteration speed bwtween index vs iterator"
  ^RealBlockVector [^ArrayList histories ^RealBlockVector x ^double lambda]
  [(with-out-str-data-map
     (time
      (let [len (.size histories)]
        (loop [i (long 0)
               acc (double 0.0)]
          (if (< i len)
            (let [h ^pct.data.ProtonHistory (.get histories i)]
              (recur (unchecked-inc i) (+ (.chord-len h) acc)))
            acc)))))
   (with-out-str-data-map
     (time
      (let [it (clojure.lang.RT/iter histories)]
        (loop [acc (double 0.0)]
          (if (.hasNext it)
            (let [h ^pct.data.ProtonHistory (.next it)]
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
                     (pct.data/proj_art-1* ^pct.data.ProtonHistory (.get histories i) x lambda))
              x)))))
     (with-out-str-data-map
       (time
        (let [len (.size histories)]
          (loop [i (long 0)
                 x x1]
            (if (< i len)
              (recur (unchecked-inc i)
                     (pct.data/proj_art-2* ^pct.data.ProtonHistory (.get histories i) x lambda))
              x)))))
     (with-out-str-data-map
       (time
        (let [len (.size histories)]
          (loop [i (long 0)
                 x x2]
            (if (< i len)
              (recur (unchecked-inc i)
                     (pct.data/proj_art-3* ^pct.data.ProtonHistory (.get histories i) x lambda))
              x)))))
     (with-out-str-data-map
         (time
          (let [len (.size histories)]
            (loop [i (long 0)
                   x x-arr]
              (if (< i len)
                (recur (unchecked-inc i)
                       (pct.data/proj_art-4* ^pct.data.ProtonHistory (.get histories i) x lambda))
                x)))))
     #_(with-out-str-data-map
         (time
          (let [len (.size histories)]
            (loop [i (long 0)]
              (if (< i len)
                (let [h ^pct.data.ProtonHistory (.get histories i)
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
                 (pct.data/proj_art-4* ^pct.data.ProtonHistory (.get histories i) x lambda))
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

#_(defn find-prime-step ^long [^long n]
  (let [s (long (if (< n 300)
                  (Math/round (* 0.618 n))
                  (Math/round (* 0.618 (double (/ n 12))))))
        plist ^ArrayList (prime-seq s)]
    (loop [i (unchecked-dec (.size plist))]
      (if (= i 0)
        1
        (let [p (long (.get plist i))]
          (if (= (rem n p) 0)
            (recur (unchecked-dec i))
            p))))))

(defn find-prime-step ^long [^long n]
  (let [s (long (if (< n 500)
                  (Math/round (* 0.618 n))
                  (Math/round (* 0.618 (double (/ n 12))))))]
    (cond
      (< 3 s) (prime/co-prime-step s n)
      (<= s 3) 1
      :else 0)))

(defn block-recon [^pct.async.node.AsyncNode node ^pct.data.HistoryIndex global-index ^RealBlockVector init-x
                   opts]
  (let [{type :type, slices :slices, in :ch-in, res :ch-out, key :key, offset-lut :local-offsets} node
        slice-offset (long (pct.data/slice-size* global-index))
        slice-count  (count slices)
        default-iterations (long 3)]
    (cond
      (= type :head)
      (a/thread
        (let [[^long offset-x ^long length ^long offset-local] (:global-offset node)
              data-len (* slice-offset slice-count)
              thread-name (format "* [%15s]" key)
              [^ArrayList histories _]  (global-index (first slices) slice-count)
              h-size  ^long (long (.size histories))
              step    ^long (find-prime-step h-size)
              ;; _ (timbre/info (format "%s: prime step = %d" thread-name step))
              shuffled-data (let [arr (object-array h-size)]
                              (when (< 0 h-size)
                                (Collections/sort histories)
                                (loop [j (long 0), i step]
                                  (if (= i 0)
                                    (aset arr j (.get histories 0))
                                    (do (aset arr j (.get histories i))
                                        (recur (unchecked-inc j) (long (mod (+ i step) h-size)))))))
                              arr)
              iterations (long (or (:iterations opts) default-iterations))
              lambda (-> opts :lambda (get slice-count))
              tvs?   (:tvs? opts)
              rows   (.rows global-index)
              cols   (.cols global-index)
              alpha  (double (or (:alpha opts) 0.75))
              tvs-N  (long (or (:tvs-N opts) 5))
              local-x ^doubles (transfer! (subvector init-x (* offset-x slice-offset) data-len)
                                          (double-array data-len))
              dump (-> opts :dump)]
          (timbre/info (format "%s%s: start: block [%2d %2d], h-size=%-7d, step=%-5d, lambda=%s, [%6d, %5d], tvs=%s"
                               _normal_ thread-name (first slices) slice-count h-size step (. float-format format lambda)
                               (* offset-x slice-offset) slice-offset
                               (if tvs? "on" "off")))
          (loop [iter (long 0), ell (long 0)]
            (let [local-x ^doubles (if tvs?
                                     (pct.tvs/ntvs-slice local-x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                                     local-x)]
              (if (< iter iterations)
                (do (dotimes [i h-size]
                      (pct.data/proj_art-5* ^pct.data.ProtonHistory (aget ^objects shuffled-data i) local-x lambda))
                    ;; (a/>!! out [key (Arrays/copyOf ^doubles local-x data-len)])
                    (async-node/send-downstream node [key local-x])
                    (timbre/info (format "%s%s, (%d) : data sent." _normal_ thread-name iter))
                    (if-let [local-x (async-node/recv-upstream node local-x :end)]
                      (do ;; (async-node/dump-data node [key (Arrays/copyOf ^doubles local-x data-len)])
                        (recur (unchecked-inc iter)
                               (+ (long (min ell iter)) (long (rand-int (Math/abs (- ell iter)))))))
                      (do (timbre/info (format "%s%s, (%d) : stopped, recon incomplete." _term_ thread-name iter)))))
                (do (timbre/info (format "%s%s, (%d) : done. Sending out local-x" _term_ thread-name iter))
                    (a/put! res [key [local-x (:global-offset node)]])
                    (a/put! res [:end key])
                    (async-node/send-downstream node [:end key])
                    (let [_ (recv-upstream node local-x :end)]
                      (timbre/info (format "%s%s, all done." _term_ thread-name)))))))))

      (= type :body)
      (a/thread
        (let [data-len    (* slice-offset slice-count)
              thread-name (format "  [%15s]" key)
              [^ArrayList histories _]  (global-index (first slices) slice-count)
              h-size  ^long   (long (.size histories))
              step    ^long   (find-prime-step h-size)
              ;; _ (timbre/info (format "%s: prime step = %d" thread-name step))
              shuffled-data (let [arr (object-array h-size)]
                              (when (< 0 h-size)
                                (Collections/sort histories)
                                (loop [j (long 0), i step]
                                  (if (= i 0)
                                    (aset arr j (.get histories 0))
                                    (do (aset arr j (.get histories i))
                                        (recur (unchecked-inc j) (long (mod (+ i step) h-size)))))))
                              arr)
              iterations (long (or (:iterations opts) default-iterations))
              lambda     (-> opts :lambda (get slice-count))
              local-x    (double-array data-len)
              ups-keys   (into #{} (keys offset-lut))
              ups-count  (count ups-keys)]
          (timbre/info (format "%s%s: start: block [%2d %2d], h-size=%-7d, step=%-5d, lambda=%s"
                               _normal_ thread-name (first slices) slice-count  h-size step (. float-format format lambda)))
          (loop [iter (long 0)]
            (if-let [local-x (recv-upstream node local-x :end)]
              (do (dotimes [i h-size]
                    (pct.data/proj_art-5* ^pct.data.ProtonHistory (aget ^objects shuffled-data i) local-x lambda))
                  #_(a/>!! out [key local-x])
                  (async-node/send-downstream node [key local-x])
                  ;; (a/>!! out [key (Arrays/copyOf local-x data-len)])
                  ;; (timbre/info (format "%s%s, (%d) : data sent." _normal_ thread-name iter))
                  (recur (unchecked-inc iter)))
              (do (timbre/info (format "%s%s, finished (iter = %d)" _term_ thread-name iter))
                  (async-node/send-downstream node  [:end key]))))))

      (= type :fake)
      (a/go
        (let [thread-name (format "...   Fake [%15s]" key)]
          (timbre/info (format "%s%s: start forwarding." _warning_ thread-name))
          (loop []
            (if-let [[k v] (a/<! in)]
              (if (= k :stop)
                (do (timbre/info (format "%s%s: stop forwarding." _term_ thread-name)) nil)
                (do #_(a/>! out [key v])
                    (async-node/send-downstream node [key v])
                    (recur)))
              (do (timbre/info (format "%s%s: input channel closed, stop forwarding." _term_ thread-name)) nil)))))
      :else
      (a/go (timbre/info (format "%s Unrecongnized type inf node [%s]" _term_ key))))))


(defn head-node-recon
  [thread-name iter ell ^doubles local-x ^pct.async.node.AsyncNode node ^ArrayList cache opts]
  (let [{slices :slices, res :ch-out, key :key} node
        ;; [^long offset-x ^long length ^long offset-local] (:global-offset node)
        dump (-> opts :dump)
        tvs? (:tvs? opts)
        [^int h-size ^objects shuffled-data ^double lambda  ^double alpha ^long tvs-N] cache]
    (timbre/info (format "%s (%d): start: block [%2d %2d], h-size=%-7d, lambda=%s, tvs=%s"
                         thread-name iter (first slices) (count slices) h-size (. float-format format lambda)
                         (if tvs? "on" "off")))
    (let [^doubles local-x (let [^doubles x (if (< 0 ^long iter)
                                              (async-node/recv-upstream node local-x :end)
                                              local-x)]
                             (if tvs?
                               (pct.tvs/ntvs-slice x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                               x))]
      (dotimes [i h-size]
        (pct.data/proj_art-5* ^pct.data.ProtonHistory (aget ^objects shuffled-data i) local-x lambda))
      ;; (a/>!! out [key (Arrays/copyOf ^doubles local-x data-len)])
      (async-node/send-downstream node [key local-x])
      #_(timbre/info (format "%s%s, (%d) : data sent." _normal_ thread-name iter)))))



(defn body-node-recon
  [thread-name iter ^doubles local-x ^pct.async.node.AsyncNode node ^ArrayList cache opts]
  (let [{res :ch-out, key :key, slices :slices} node
        ;; [^long offset-x ^long length ^long offset-local] (:global-offset node)
        [^int h-size ^objects shuffled-data ^double lambda] cache]
    #_(timbre/info (format "%s (%d): start: block [%2d %2d], h-size=%-7d, lambda=%s"
                         thread-name iter (first slices) (count slices) h-size (. float-format format lambda)))
    (let [^doubles local-x (async-node/recv-upstream node local-x :end)]
      (dotimes [i h-size]
        (pct.data/proj_art-5* ^pct.data.ProtonHistory (aget ^objects shuffled-data i) local-x lambda))
      (async-node/send-downstream node [key local-x])
      #_(timbre/info (format "%s%s, (%d) : data sent." _normal_ thread-name iter)))))


(defn thread-recon
  [node-list ^pct.data.HistoryIndex global-index ^RealBlockVector init-x opts]
  (a/thread
    (let [head-node  (first node-list)
          body-nodes (next  node-list)
          [^long offset-x ^long length ^long offset-local] (:global-offset head-node)
          ^long slice-offset (:offset head-node)
          ;; rows (.rows global-index), cols (.cols global-index)
          thread-name (format "[%3s]" (-> node-list first :key))
          tvs? (:tvs? opts)
          iterations    (long (or (:iterations opts) 6))
          alpha (double (or (:alpha opts) 0.75))
          tvs-N (long   (or (:tvs-N opts) 5))
          ;; slice-offset (long (pct.data/slice-size* global-index))
          ]
      (let [local-x (transfer! (subvector init-x (* offset-x slice-offset) slice-offset)
                               (double-array (* slice-offset ^long (reduce max (mapv #(%) node-list)))))
            caches  (mapv (fn [node]
                            (let [ ;; cache (HashMap.)
                                  slices (:slices node)
                                  slice-count (count slices)
                                  ^ArrayList histories (first (global-index (first slices) slice-count))
                                  h-size (.size histories)
                                  step (find-prime-step h-size)
                                  ^double lambda (-> opts :lambda (get slice-count))
                                  ^objects shuffled-data (let [arr (object-array h-size)]
                                                           (when (< 0 h-size)
                                                             (Collections/sort histories)
                                                             (loop [j (long 0), i step]
                                                               (if (= i 0)
                                                                 (aset arr j (.get histories 0))
                                                                 (do (aset arr j (.get histories i))
                                                                     (recur (unchecked-inc j) (long (mod (+ i step) h-size))))))
                                                             arr))]
                              ;; (println slices slice-count h-size #_(alength shuffled-data) #_(alength local-x))
                              ;; (println (alength shuffled-data))
                              [h-size shuffled-data lambda alpha tvs-N]))
                          node-list)]
        (loop [iter (long 0), ell (long 0)]
          (if (< iter iterations)
            (do #_(timbre/info (format "%s: iter=%d" thread-name iter))
                (head-node-recon thread-name iter ell local-x head-node (caches 0) opts)
                (loop [i (long 1), body body-nodes]
                  (if-let [node (first body)]
                    (do (body-node-recon thread-name iter local-x node (caches i) opts)
                        (recur (unchecked-inc i) (next body)))))
                #_(timbre/info (format "%s: end of iter %d" thread-name iter))
                (recur (unchecked-inc iter)
                       (+ (long (min ell iter)) (long (rand-int (Math/abs (- ell iter)))))))
            (let [final-ell (+ (long (min ell iter)) (long (rand-int (Math/abs (- ell iter)))))
                  ^doubles local-x (if tvs?
                                     (pct.tvs/ntvs-slice local-x [rows cols] final-ell :alpha alpha :N tvs-N :in-place true)
                                     local-x)]
              (timbre/info (format "%s: done. Waiting for final iter" thread-name))
              (async-node/recv-upstream head-node local-x)
              (timbre/info (format "%s: %s done." thread-name (:slices head-node)))
              (let [out (:ch-out head-node)
                    key (:key    head-node)]
                (a/put! out [key [local-x (:global-offset head-node)]])
                (a/put! out [:end key])))))))))


(defn async-art-blocked [^pct.async.node.AsyncGrid grid ^pct.data.HistoryIndex global-index ^RealBlockVector init-x opts]
  {:pre [(if-let [iter (:iterations opts)]
           (spec/valid? (spec/and pos? int?) iter)
           true)]
   :post []}
  (async-node/distribute-all grid block-recon [global-index init-x (dissoc opts :dump)])
  (let [res-ch (async-node/collect-data grid)
        slice-offset (long (pct.data/slice-size* global-index))
        final-x ^RealBlockVector (zero init-x)]
    (loop []
      (if-let [[k msg] (a/<!! res-ch)]
        (let [n (count msg)]
          (if (= n 2)
            (let [[^doubles arr [^long global-offset ^long len _]] msg
                  v (subvector final-x (* global-offset slice-offset) (* len slice-offset))]
              (transfer! arr v)
              (recur))
            (do (println (format "Message format is wrong, expect n=2, got n=%d. Skip." n))
                (recur))))
        final-x))))


(defn async-art-threaded [^pct.async.node.AsyncGrid grid ^pct.data.HistoryIndex global-index ^RealBlockVector init-x opts]
  {:pre [(if-let [iter (:iterations opts)]
           (spec/valid? (spec/and pos? int?) iter)
           true)]
   :post []}
  (async-node/distribute-heads grid thread-recon [global-index init-x (dissoc opts :dump)])
  (let [res-ch (async-node/collect-data grid)
        slice-offset (long (pct.data/slice-size* global-index))
        final-x ^RealBlockVector (zero init-x)]
    (loop []
      (if-let [[k msg] (a/<!! res-ch)]
        (let [n (count msg)]
          (if (= n 2)
            (let [[^doubles arr [^long global-offset ^long len _]] msg
                  v (subvector final-x (* global-offset slice-offset) (* len slice-offset))]
              (transfer! arr v)
              (recur))
            (do (println (format "Message format is wrong, expect n=2, got n=%d. Skip." n))
                (recur))))
        final-x))))


(defn async-art-test [^pct.async.node.AsyncGrid grid ^pct.data.HistoryIndex global-index ^RealBlockVector init-x opts]
  {:pre [(if-let [iter (:iterations opts)]
           (spec/valid? (spec/and pos? int?) iter)
           true)]
   :post []}
  (async-node/distribute-all grid block-recon [global-index init-x opts])
  (let [res-ch  (async-node/collect-data grid)
        slice-offset (long (pct.data/slice-size* global-index))]
    (loop [acc (transient {})]
      (if-let [[^clojure.lang.Keyword k msg] (a/<!! res-ch)]
        (let [n (count msg)]
          (cond
            (= n 2)
            (let [[^doubles arr [^long global-offset ^long len ^long local-offset]] msg
                  [^RealBlockVector final-x slices] (or (get acc :final) [(zero init-x) (sorted-set)])
                  v (subvector final-x (* global-offset slice-offset) slice-offset)]
              (transfer! arr v)
              #_(println slices (-> grid k :slices) (set/union slices (-> grid k :slices)))
              (recur (assoc! acc :final [final-x (set/union slices (-> grid k :slices))])))

            (= n 3)
            (let [[^long iter ^doubles arr [^long global-offset ^long len ^long local-offset]] msg
                  [^RealBlockVector x slices] (or (get acc iter) [(zero init-x) (sorted-set)])
                  v (subvector x (* global-offset slice-offset) slice-offset)]
              (transfer! arr v)
              (recur (assoc! acc iter [x (set/union slices (-> grid k :slices))])))

            :else
            (recur acc)))
        (persistent! acc)))))


(defn art-block [^ArrayList histories ^doubles init-x opts]
  {:pre [(if-let [iter (:iterations opts)]
           (spec/valid? (spec/and pos? int?) iter)
           true)]
   :post []}
  (let [data-len ^int (alength init-x)
        local-x  ^doubles (Arrays/copyOf init-x data-len)
        h-size   ^int (.size histories)
        step     ^long (find-prime-step h-size)
        tvs?     (:tvs? opts)
        alpha    (double (or (:alpha opts) 0.75)) ;; from Blake's paper 0.75 or 0.05
        tvs-N    (long   (or (:tvs-N opts) 5))
        [^long rows ^long cols] (:dim opts)
        shuffled-data (let [arr (object-array h-size)]
                              (when (< 0 h-size)
                                (Collections/sort histories)
                                (loop [j (long 0), i step]
                                  (if (= i 0)
                                    (aset arr j (.get histories 0))
                                    (do (aset arr j (.get histories i))
                                        (recur (unchecked-inc j) (long (mod (+ i step) h-size)))))))
                              arr)
        iterations (long (or (:iterations opts) (long 3)))
        lambda     (double (or (:lambda opts) 0.001))]
    (loop [iter (long 0)
           ell  (long 0)
           acc (transient {})]
      (if (< iter iterations)
        (let [local-x (if tvs?
                        (pct.tvs/ntvs-slice local-x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                        local-x)]
          (dotimes [i h-size]
            (pct.data/proj_art-5* ^pct.data.ProtonHistory (aget ^objects shuffled-data i) local-x lambda))
          (let [next-iter (unchecked-inc iter)
                next-ell (+ (long (min ell iter)) (long (rand-int (Math/abs (- ell iter)))))]
            (recur next-iter next-ell
                   (assoc! acc next-iter (Arrays/copyOf ^doubles local-x data-len)))))
        (persistent! (assoc! acc :final (if tvs?
                                          (pct.tvs/ntvs-slice local-x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                                          local-x)))))))


(defn seq-art [^pct.async.node.AsyncGrid grid ^pct.data.HistoryIndex global-index ^RealBlockVector init-x opts]
  (timbre/info "seq-art start")
  (let [histories ^ArrayList (reduce (fn [^ArrayList acc node]
                                       (let [s (:slices node)]
                                         #_[(first s) (count s)]
                                         (.addAll acc (first (global-index (first s) (count s))))
                                         acc))
                                     (ArrayList.)
                                     (seq grid))
        h-size (.size histories)
        step ^long (find-prime-step h-size)
        shuffled-data (let [arr (object-array h-size)]
                              (when (< 0 h-size)
                                (Collections/sort histories)
                                (loop [j (long 0), i step]
                                  (if (= i 0)
                                    (aset arr j (.get histories 0))
                                    (do (aset arr j (.get histories i))
                                        (recur (unchecked-inc j) (long (mod (+ i step) h-size)))))))
                              arr)
        iterations (long (or (:iterations opts) 3))
        lambda (-> opts :lambda)
        tvs?   (:tvs? opts)
        rows   (.rows global-index)
        cols   (.cols global-index)
        alpha  (double (or (:alpha opts) 0.75))
        tvs-N  (long (or (:tvs-N opts) 5))
        data-len (dim init-x)
        local-x ^doubles (transfer! init-x (double-array data-len))]
    (timbre/info (format "start sequential art recon: history size = %d, shuffle step = %d, iteration = %d, lambda = %f"
                         h-size step iterations lambda) )
    (loop [iter (long 0)
           ell  (long 0)]
      (if (< iter iterations)
        (let [local-x ^doubles (if tvs?
                                 (pct.tvs/ntvs-slice local-x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                                 local-x)]
          (dotimes [i h-size]
            (pct.data/proj_art-5* ^pct.data.ProtonHistory (aget ^objects shuffled-data i) local-x lambda))
          (timbre/info (format "iterateion %d done." iter))
          (recur (unchecked-inc iter)
                 (+ (long (min ell iter)) (long (rand-int (Math/abs (- ell iter)))))))
        local-x))))


