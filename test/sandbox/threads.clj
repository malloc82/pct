(ns sandbox.threads
  (:use clojure.core)
  (:require [clojure.core.async :as a]
            [clojure.string :as s]
            [clojure.spec.alpha :as spec]
            [taoensso.timbre :as logger])
  (:import [java.util Iterator HashMap ArrayList ArrayDeque HashSet]))

(set! *warn-on-reflection* true)


(def ch0 (a/chan 1))
(def ch1 (a/chan 1))
(def ch2 (a/chan 1))

(defn- mkIdxKey [ids]
  (keyword (s/join "-" ids)))


(defn- my-mix
  "Simplified mix function"
  [out]
  (let [cs (atom {}) ;;ch->name
        q  (atom [])
        push (fn [k v]
               (let [it (clojure.lang.RT/iter @q)]
                 (loop [idx 0]
                   (if (.hasNext it)
                     (let [m (.next it)]
                       (if (m k)
                         (recur (inc idx))
                         (swap! q assoc-in [idx k] v)))
                     (swap! queue conj {k v}))))
               (let [m (first @q)]
                 (when (= (count m) (count @cs))
                   (swap! q #(subvec % 1))
                   m)))
        change (a/chan (a/sliding-buffer 1))
        changed #(a/put! change true)
        calc-state (fn []
                     (let [chs (keys @cs)]
                       {:reads (conj chs change)
                        :chs   (set (vals @cs))}))
        m (reify
            a/Mux
            (muxch* [_] out)
            a/Mix
            (admix* [_ [ch k]] (swap! cs assoc ch k) (changed))
            (unmix* [_ ch] (swap! cs dissoc ch) (changed))
            (unmix-all* [_] (reset! cs {}) (changed))
            #_#_(toggle* [_ _])
            (solo-mode* [_ _]))]
    (a/go-loop [{:keys [reads chs] :as state} (calc-state)]
      (let [[v c] (a/alts! reads)
            k     (get @cs c)]
        (if (or (nil? v) (= c change))
          (do (when (nil? v)
                (swap! cs dissoc c))
              (recur (calc-state)))
          (if-let [data (push k v)]
            (when (a/>! out data)
              (recur state))
            (recur state)))))
    m))

(defn- wait-cs!
  "Wait for a list of channels, async"
  [cs-map]
  (let [ks (set (keys cs-map))
        n  (count cs-map)
        read-out (a/chan n)
        mx (a/mix read-out)]
    (doseq [[k c] cs-map]
      (a/admix mx c))
    (a/go-loop [result {}]
      (if-let [[k v] (a/<! read-out)]
        (if (ks k)
          (let [res (assoc result k v)]
            (if (= (count res) n)
              (do (a/unmix-all mx)
                  res)
              (recur res)))
          (recur result))))))


(defn- wait-cs!!
  "Wait for a list of channels, blocking"
  [cs-map]
  (let [n  (count cs-map)
        read-out (a/chan n)
        mx (a/mix read-out)
        ks (set (keys cs-map))]
    (doseq [[k c] cs-map]
      (a/admix mx c))
    (loop [result {}]
      (if-let [[k v] (a/<!! read-out)]
        (if (ks k)
          (let [res (assoc result k v)]
            (if (= (count result) n)
              (do (a/unmix-all mx)
                  res)
              (recur res)))
          (recur result))))))


(def q (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn push-fn [atm k v]
  (swap! atm (fn [a k v]
               (let [it (clojure.lang.RT/iter a)]
                 (loop []
                   (if (.hasNext it)
                     (let [m ^HashMap (.next it)]
                       (if (.get m k)
                         (recur)
                         (do (.put m k v)
                             a)))
                     (conj a (HashMap. {k v}))))))
         k v))

(defn pop-fn [atm]
  (let [v (first @atm)]
    (swap! atm pop)
    (into {} v)))

;; (def b (atom (ArrayDeque.)))

(defn push-fn2 [atm k v]
  (let [st ^ArrayDeque @atm]
    (loop [it ^Iterator (.iterator st)]
      (if (.hasNext it)
        (let [m ^HashMap (.next it)]
          (if (.get m k)
            (recur it)
            (.put m k v)))
        ()))))



;; Map test
(let [ITER 10000 ;; iterations
      N    1000  ;; number of items to be inserted
      SIZE 10    ;; number of entries in the map
      ,]
  (time (dotimes [_ ITER]
          (loop [m {}
                 n 0]
            (if (< n N)
              (recur (assoc m (rand-int SIZE) n) (inc n))
              #_(println m)))))

  (time (dotimes [_ ITER]
          (let [m ^HashMap (HashMap.)]
            (loop [n 0]
              (if (< n SIZE)
                (do (.put m (rand-int SIZE) n)
                    (recur (inc n)))
                #_(println m)))))))

()

(spec/def ::channel? #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(spec/def ::keyword? #(instance? clojure.lang.Keyword %))
(spec/def ::map? #(or (map? %) (instance? HashMap %)))
(spec/def ::named-chans? (spec/map-of ::channel? ::keyword?))
(spec/def ::unique-vals? (fn [m]
                           (= (count (set (vals m))) (count m))))
(spec/def ::channel->name? (spec/and ::map? ::unique-vals? ::named-chans?))


(defn- sync-data
  "in-chs : a map of input channels with corresponding names ({ch -> name}) to synchronize
   out-ch : output channel where synced data will be sent to, output value will be a map of {name -> value}
            output value will only be sent once data from all channels are collected.

   Go thread will shutdown when either all input channels are closed (normal) or output channel is closed unexpectedly. "
  [in-chs]
  (let [out-ch (a/chan 4)]
    (a/go
      (let [chs ^HashMap    (HashMap. in-chs) ;; {ch -> name}
            q   ^ArrayDeque (ArrayDeque.)
            push (fn [k v]
                   (let [it ^Iterator (.iterator q)]
                    (loop []
                      (if (.hasNext it)
                        (let [m ^HashMap (.next it)]
                          (if (.putIfAbsent m k v)
                            (recur)))
                        (.add q (HashMap. {k v}))))
                    ;; check if the first data is full, if full, return and pop head
                    (let [head ^HashMap (.peek q)]
                      (if (.containsAll ^HashSet (.keySet head) (vals chs))
                        (do (.pop q)
                            (a/put! out-ch (into {} head)))
                        true))))
            clean-up (fn []
                       (loop []
                         (if (not (.isEmpty q))
                           (when (a/put! out-ch (into {} ^HashMap (.pop q)))
                             (recur)))))
            ,]
        (loop []
          (let [[v c] (a/alts! (keys chs))
                k     (.get chs c)]
            (if (nil? v)
              (do (.remove chs c)
                  (if (.isEmpty chs)
                    (do (logger/info "sync-data : all input channels are closed. shutting down.")
                        (clean-up))
                    (recur)))
              (if (push k v)
                (recur)
                (logger/info "sync-data : out-ch closed, go-thread stopped.")))))))
    out-ch))

(spec/fdef ::sync-data
  :args (spec/cat :in-chs ::channel->name?)
  :ret  ::chan?)


(def ch-in0 (a/chan 3))
(def ch-in1 (a/chan 3))
(def ch-in2 (a/chan 3))

(def ch-out (a/chan 3))

(a/go-loop []
  (if-let [v (a/<! ch-out)]
    (do (println v)
        (recur))
    (println "\nch-out closed.")))

(sync-data {ch-in0 :0
            ch-in1 :1
            ch-in2 :2}
           ch-out)

(a/offer! ch-in0 10)
(a/offer! ch-in1 11)
(a/offer! ch-in2 12)


(a/offer! ch-in0 20)
(a/offer! ch-in0 30)
(a/offer! ch-in0 40)

(a/offer! ch-in1 21)
(a/offer! ch-in1 31)

(a/offer! ch-in2 22)


(do (a/close! ch-in0)
    (a/close! ch-in1)
    (a/close! ch-in2))
