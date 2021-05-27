(ns pct.async.threads
  (:use clojure.core)
  (:require [clojure.core.async :as a]
            [pct.util.system :as sys]
            [taoensso.timbre :as timbre]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

;; (def ^:private thread-queue (let [n  sys/LogicalCores
;;                                   ch (a/chan n)]
;;                               (dotimes [_ n]
;;                                 (a/>!! ch []))
;;                               ch))

;; (defn launchWorker [name f args]
;;   (let [ticket (a/<!! thread-queue)
;;         res-ch (a/chan 1)]
;;     (a/thread
;;       (let [res (apply f args)]
;;         (a/>!! res-ch res)
;;         (a/>!! thread-queue (conj ticket name))))
;;     res-ch))

(defn asyncWorkers
  "Spawn n worker threads, each worker continously try to take data from 'from' channel,
   process the data by calling (xf data), and send the result to acc-fn to process.
   'acc-fn' is run in another independent thread. When all data are processed, result will
   be sent to the output channel which will be return immediately upon callin asyncWorkers."

  [n xf acc-fn from]
  (let [out-ch (vec (repeatedly n #(a/chan n)))
        to (a/chan 1)]
    ;; worker threads
    (dotimes [i n]
      (a/thread
        (taoensso.timbre/info (format "thread %d started." i))
        (try
          (let [out (out-ch i)]
            (loop []
              (if-let [job (a/<!! from)]
                (do #_(taoensso.timbre/info (format "thread [%d]: got job to do." i))
                    (let [res (xf job)]
                      (a/>!! out res)
                      (recur)))
                (do (a/close! out)
                    (taoensso.timbre/info (format "thread [%d]: 'from' channel closed, thread shutting down." i))))))
          (catch Exception ex
            (taoensso.timbre/error ex (format "Something went wrong in worker [%d]" i) (.getName (Thread/currentThread)))))))
    ;; collector threads
    (a/thread
      (try
        (loop [acc (acc-fn)
               cs (into #{} out-ch)
               outs out-ch]
          (if (empty? cs)
            (do (taoensso.timbre/info (format "collector thread: 'out-ch' all closed, job all done."))
                (a/>!! to (acc-fn acc))
                (a/close! to)
                nil)
            (let [[v p] (a/alts!! outs)]
              (if v
                (do #_(taoensso.timbre/info (format "collector thread: got data"))
                    (recur (acc-fn acc v) cs outs))
                (let [new-cs (disj cs p)]
                  (recur acc new-cs (vec new-cs)))))))
        (catch Exception ex
          (a/close! to)
          (taoensso.timbre/error ex "[asyncWorker] Something went wrong in collector thread: " (.getName (Thread/currentThread))))))
    to))


(defn asyncAccumulator
  "Spawn n worker threads, each starts by calling (acc+xf) to acquire a 'xf' function with accumulator in closure.
   For every piece of data each thread obtained is sent to local accumulator by calling (xf data).
   When the input channel 'from' is closed, each thread will get their local accumulator by calling (xf) and send it
   to acc-fn to process.

   'acc-fn' will only get one item from each thread."

  [n acc+xf acc-fn from]
  (let [out-ch (vec (repeatedly n #(a/chan n)))
        to (a/chan 1)]
    ;; worker threads
    (dotimes [i n]
      (a/thread
        (taoensso.timbre/info (format "thread %d started." i))
        (try
          (let [out (out-ch i)
                xf  (acc+xf)]
            (loop []
              (if-let [job (a/<!! from)]
                (do #_(taoensso.timbre/info (format "thread [%d]: got job to do." i))
                    (let [_ (xf job)]
                      #_(a/>!! out res)
                      (recur)))
                (do (a/>!! out (xf))
                    (a/close! out)
                    (taoensso.timbre/info (format "thread [%d]: 'from' channel closed, thread shutting down." i))))))
          (catch Exception ex
            (taoensso.timbre/error ex (format "Something went wrong in worker [%d]" i) (.getName (Thread/currentThread)))))))
    ;; collector threads
    (a/thread
      (try
        (loop [acc (acc-fn)
               cs (into #{} out-ch)
               outs out-ch]
          (if (empty? cs)
            (do (taoensso.timbre/info (format "collector thread: 'out-ch' all closed, job all done."))
                (a/>!! to (acc-fn acc))
                (a/close! to)
                nil)
            (let [[v p] (a/alts!! outs)
                  new-cs (disj cs p)]
              (if v
                (recur (acc-fn acc v) new-cs (vec new-cs))
                (recur acc new-cs (vec new-cs))))))
        (catch Exception ex
          (a/close! to)
          (taoensso.timbre/error ex "[asyncAccumulator] Something went wrong in collector thread: " (.getName (Thread/currentThread))))))
    to))



