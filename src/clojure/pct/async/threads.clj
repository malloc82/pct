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

(defn asyncWorkers [n xf acc-fn from]
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
                (do (taoensso.timbre/info (format "thread [%d]: got job to do." i))
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
                (do (taoensso.timbre/info (format "collector thread: got data"))
                    (recur (acc-fn acc v) cs outs))
                (let [new-cs (disj cs p)]
                  (recur acc new-cs (vec new-cs)))))))
        (catch Exception ex
          (taoensso.timbre/error ex "Something went wrong in collector thread: " (.getName (Thread/currentThread))))))
    to))
