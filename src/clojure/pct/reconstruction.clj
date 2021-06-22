(ns pct.reconstruction
  (:use clojure.core)
  (:require pct.data pct.async.node
            pct.tvs
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
  (:import [java.util ArrayList Iterator Arrays Collections]
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
  (let [{type :type, slices :slices, in :ch-in, out :ch-out, res :ch-log, key :key, offset-lut :local-offsets} node
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
          (loop [iter (long 0)
                 ell  (long 0)]
            (if (< iter iterations)
              (let [local-x ^doubles (if tvs?
                                       (pct.tvs/ntvs-slice local-x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                                       local-x)]
                  (dotimes [i h-size]
                    (pct.data/proj_art-5* ^pct.data.PathData (aget ^objects shuffled-data i) local-x lambda))
                  #_(a/>!! out [key local-x])
                  (a/>!! out [key (Arrays/copyOf ^doubles local-x data-len)])
                  (timbre/info (format "%s%s, (%d) : data sent." _normal_ thread-name iter))
                  (let [continue?
                        (loop [remaining (into #{} (keys offset-lut))]
                          (if (empty? remaining)
                            (do (if dump
                                  (let [_copy_ (Arrays/copyOf ^doubles local-x data-len)]
                                    (a/put! res [key [(unchecked-inc iter) _copy_ (:global-offset node)]])))
                                true)
                            (if-let [[^clojure.lang.Keyword k ^doubles v] (a/<!! in)]
                              (if-let [[^long offset-v ^long length ^long offset-local] (k offset-lut)]
                                (do (timbre/info (format "%s%s, (%d) : received data from %s" _normal_ thread-name iter k))
                                    (System/arraycopy v       (* offset-v     slice-offset)
                                                      local-x (* offset-local slice-offset)
                                                      (* length slice-offset))
                                    (recur (disj remaining k)))
                                (do (timbre/info (format "%s%s, (%d) : could not find key %s, skip."
                                                         _normal_ thread-name iter k))
                                    (recur remaining)))
                              (do (timbre/info (format "%s%s, (%d) : incoming channel is closed."
                                                       _term_ thread-name iter))
                                  nil))))]
                    (if continue?
                      (recur (unchecked-inc iter)
                             (+ (long (min ell iter)) (long (rand-int (Math/abs (- ell iter))))))
                      (do (timbre/info (format "%s%s, (%d) : stopped, recon incomplete." _term_ thread-name iter))))))
              (let [local-x ^doubles (if tvs?
                                       (pct.tvs/ntvs-slice local-x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                                       local-x)]
                (timbre/info (format "%s%s, (%d) : done. Sending out local-x" _term_ thread-name iter))
                (a/>!! res [key [local-x (:global-offset node)]])
                (a/>!! res [:end]))))))

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
              local-x    (double-array data-len)]
          (timbre/info (format "%s%s: start: block [%2d %2d], h-size=%-7d, step=%-5d, lambda=%s"
                               _normal_ thread-name (first slices) slice-count  h-size step (. float-format format lambda)))
          (loop [iter (long 0)]
            (if (< iter iterations)
              (let [continue?
                    (loop [remaining (into #{} (keys offset-lut))]
                      (if (empty? remaining)
                        (do (dotimes [i h-size]
                              (pct.data/proj_art-5* ^pct.data.PathData (aget ^objects shuffled-data i) local-x lambda))
                            #_(a/>!! out [key local-x])
                            (a/>!! out [key (Arrays/copyOf local-x data-len)])
                            (timbre/info (format "%s%s, (%d) : data sent." _normal_ thread-name iter))
                            true)
                        (if-let [[^clojure.lang.Keyword k ^doubles v] (a/<!! in)]
                          (if-let [[^long offset-v ^long length ^long offset-local] (k offset-lut)]
                            (do (timbre/info (format "%s%s, (%d) : received data from %s" _normal_ thread-name iter k))
                                (System/arraycopy v       (* offset-v     slice-offset)
                                                  local-x (* offset-local slice-offset)
                                                  (* length slice-offset))
                                (recur (disj remaining k)))
                            (do (timbre/info (format "%s%s, (%d) : could not find key %s, skip."
                                                     _warning_ thread-name iter k))
                                (recur remaining)))
                          (do (timbre/info (format "%s%s, (%d) : incoming channel is closed."
                                                   _term_ thread-name iter))
                              nil))))]
                (if continue?
                  (recur (unchecked-inc iter))
                  (do (timbre/info (format "%s%s, (%d) : stopped, recon incomplete." _term_ thread-name iter)))))
              (do (timbre/info (format "%s%s, (%d) : finished." _term_ thread-name iter)))))))

      (= type :fake)
      (a/go
        (let [thread-name (format "...   Fake [%15s]" key)]
          (timbre/info (format "%s%s: start forwarding." _warning_ thread-name))
          (loop []
            (if-let [[k v] (a/<! in)]
              (if (= k :stop)
                (do (timbre/info (format "%s%s: stop forwarding." _term_ thread-name)) nil)
                (do (a/>! [key v])
                    (recur)))
              (do (timbre/info (format "%s%s: input channel closed, stop forwarding." _term_ thread-name)) nil)))))
      :else
      (a/go (timbre/info (format "%s Unrecongnized type inf node [%s]" _term_ key))))))


(defn async-art [^pct.async.node.AsyncGrid grid ^pct.data.HistoryIndex global-index ^RealBlockVector init-x opts]
  {:pre [(if-let [iter (:iterations opts)]
           (spec/valid? (spec/and pos? int?) iter)
           true)]
   :post []}
  (pct.async.node/distribute-all grid block-recon [global-index init-x (dissoc opts :dump)])
  (let [res-ch (pct.async.node/collect-data grid)
        slice-offset (long (pct.data/slice-size* global-index))
        final-x ^RealBlockVector (zero init-x)]
    (loop []
      (if-let [[k msg] (a/<!! res-ch)]
        (let [n (count msg)]
          (if (= n 2)
            (let [[^doubles arr [^long global-offset ^long len _]] msg
                  v (subvector final-x (* global-offset slice-offset) slice-offset)]
              #_(println [k global-offset len slice-offset])
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
  (pct.async.node/distribute-all grid block-recon [global-index init-x opts])
  (let [res-ch  (pct.async.node/collect-data grid)
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
            (pct.data/proj_art-5* ^pct.data.PathData (aget ^objects shuffled-data i) local-x lambda))
          (let [next-iter (unchecked-inc iter)
                next-ell (+ (long (min ell iter)) (long (rand-int (Math/abs (- ell iter)))))]
            (recur next-iter next-ell
                   (assoc! acc next-iter (Arrays/copyOf ^doubles local-x data-len)))))
        (persistent! (assoc! acc :final (if tvs?
                                          (pct.tvs/ntvs-slice local-x [rows cols] ell :alpha alpha :N tvs-N :in-place true)
                                          local-x)))))))
