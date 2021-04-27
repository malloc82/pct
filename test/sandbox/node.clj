(ns sandbox.node
  (:use clojure.core #_pct.common)
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.inspector :as inspector])
  (:import [java.util HashMap TreeSet LinkedList]
           [java.util.concurrent.locks Lock]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(spec/def ::channel? #(instance? clojure.core.async.impl.channels.ManyToManyChannel %))
(spec/def ::continuous? (fn [col]
                           (case (count col)
                             0 false
                             1 true
                             (let [it (clojure.lang.RT/iter col)]
                               (loop [a (.next it)
                                      b (.next it)]
                                 (if (= (- b a) 1)
                                   (if (.hasNext it)
                                     (recur b (.next it))
                                     true)
                                   false))))))


(spec/def ::slice-id? (spec/and int? #(not (neg? %))))
(spec/def ::continuous-slice-ids? (spec/and (spec/coll-of ::slice-id?)
                                            ::continuous?))

(spec/def ::atomic? #(inspector/atom? %))
(spec/def ::atomic-slice-ids? (spec/and ::atomic? #(let [s %]
                                                     (spec/coll-of ::slice-id? @s))))

(defn indexOf [coll elem]
  (let [it (clojure.lang.RT/iter coll)]
    (loop [i (int 0)]
      (if (.hasNext it)
        (let [curr (.next it)]
          (if (= curr elem)
            i
            (recur (unchecked-inc-int i))))
        -1))))

#_(defn sharedSlices [slices unused]
  {:pre [(spec/valid? ::continuous-slice-ids? slices)
         (spec/valid? ::continuous-slice-ids? unused)]
   :post []}
  (let [iter-s (clojure.lang.RT/iter slices)
        iter-u (clojure.lang.RT/iter unused)]
    (if (and (.hasNext iter-s) (.hasNext iter-u))
      (loop [s (.next iter-s)
             u (.next iter-u)]
        ))))

(defprotocol ICommunication
  (get-overlap [upstream downstream]   "given an downstream node, find the overlapping slices")
  (connect-upstream [this upstream]    "set up a connection with an upstream node")
  (disconnect-upstream [this upstream] "disconnect with the upstream node"))

(spec/def ::async-node? (spec/keys :req-un [::data-in ::cmd-in ::__out ::data-out
                                            ::key ::slices
                                            ::used ::unused
                                            ::upstream ::downstream
                                            ::iter ::state
                                            ::property]))


;; (deftype NodesTracking [used unused upstream downstream ^Lock mutex])

(defrecord AsyncNode [data-in cmd-in __out data-out  ;; commication
                      key slices
                      used unused         ;; tracking slice usage
                      upstream downstream ;; tracking connection, nodes
                      iter state ;; work thread related
                      properties ;; reconnstruction properties, e.g. each node could have different lambda parameter
                      ,]
  ICommunication
  (get-overlap [this downstream]
    (let [it     (clojure.lang.RT/iter @unused)
          slices (:slices downstream)]
      (loop [usage (sorted-set)
             state :SEARCH
             prev-match (int 0)]
        (case state
          :SEARCH  (if (.hasNext it)
                     (let [s (int (.next it))]
                       (if (slices s)
                         (recur (conj usage s) :FOUND s)
                         (recur usage :SEARCH prev-match)))
                     usage)
          :FOUND (if (.hasNext it)
                   (let [s (int (.next it))]
                     (if (and (slices s) (= (- s prev-match) 1)) ;; making sure next slice is a continuous from previous one
                       (recur (conj usage s) :FOUND s)
                       usage))
                   usage)))))
  (connect-upstream [this upstream-node]
    (let [usage (get-overlap upstream-node this)#_(set/intersection slices @(:unused other-node))]
      (if-not (empty? usage)
        (let [idx   (indexOf (:slices upstream-node) (first usage))]
          (swap! (:unused     upstream-node) #(set/difference % usage))
          (swap! (:downstream upstream-node) assoc key usage)
          (a/tap (:data-out   upstream-node) data-in)
          (swap! upstream assoc (:key upstream-node) [usage [idx (count usage)]])
          true)
        false)))
  (disconnect-upstream [this upstream-node]
    (swap! upstream (fn [ups]
                      (if-let [[usage _] (get ups (:key upstream-node))]
                        (do (swap!   (:unused     upstream-node) #(apply conj % usage))
                            (swap!   (:downstream upstream-node) dissoc key)
                            (a/untap (:data-out   upstream-node) data-in)
                            (dissoc ups (:key upstream-node)))
                        ups)))
    #_(if-let [usage (get @upstream (:key other-node))]
      (do (swap! (:unused other-node) #(apply conj % usage))
          (swap! upstream dissoc (:key other-node))
          (swap! (:downstream other-node) dissoc key)
          (a/untap (:data-out other-node) data-in))
      (when-let [usage (get @downstream (:key other-node))]
        (do (swap! unused set/union usage)
            (swap! downstream dissoc (:key other-node))
            (swap! (:upstream other-node) dissoc key)
            (a/untap data-out (:data-in other-node)))))))


(defrecord NodeState [status parents])

(defrecord NodeDB [LUT TIERED])

(defn newNode [slices]
  {:pre [(spec/valid? ::continuous-slice-ids? slices)]
   :post []}
  (let [out (a/chan 2)]
    (map->AsyncNode {:data-in  (a/chan 2)
                     :cmd-in   (a/chan 2)
                     :__out    out
                     :data-out (a/mult out)
                     :key      (keyword (clojure.string/join "-" slices))
                     :slices   (into (sorted-set) slices)
                     ;; :used     (atom {})
                     :unused   (atom (into (sorted-set) slices))
                     :upstream   (atom {})
                     :downstream (atom {})
                     :properties {}
                     ;; states
                     :iter     (atom (int 0))
                     :state    (atom (map->NodeState {:status :init :parents []}))
})))

;; (def Tier1 (->> (range 16)
;;                 (mapv #(newNode [%]))))
;; (map (fn [n] {:key (:key n) :slices (:slices n)}) Tier1)

;; (def Tier2-1 (->> (range 16)
;;                 (filter even?)
;;                 (mapv (fn [n] (newNode [n (inc n)])))))
;; (map (fn [n] {:key (:key n) :slices (:slices n)}) Tier2-1)

;; (def Tier2-2 (->> (range 16)
;;                   (filter odd?)
;;                   (mapv (fn [n] (newNode [n (inc n)])))))

;; (def Tier3-1 (->> (range 16)
;;                   (filter #(and (= (mod % 3) 0) (<= (+ % 3) 16)))
;;                   (mapv (fn [n] (newNode (range n (+ n 3)))))))

;; (def Tier3-2 (->> (range 16)
;;                   (filter #(and (= (mod % 3) 1) (<= (+ % 3) 16)))
;;                   (mapv (fn [n] (newNode (range n (+ n 3)))))))

;; (def Tier3-3 (->> (range 16)
;;                   (filter #(and (= (mod % 3) 2) (<= (+ % 3) 16)))
;;                   (mapv (fn [n] (newNode (range n (+ n 3)))))))


;; (defn generateNode [slices tiers]
;;   (let [slice-idx (range slices)
;;         tier-gen  (fn [t]
;;                     (mapv (fn [n]
;;                             (->> slice-idx
;;                                  (filter #(and (= (mod % t) n) (<= (+ % t) slices)))
;;                                  (mapv (fn [s] (newNode (range s (+ s t)))))))
;;                           (range t)))
;;         ,]
;;     (mapv #(tier-gen (inc %)) (range tiers))))


;; (def nodes (generateNode 16 3))

(defn newDB [slices tiers]
  (let [node-map ^HashMap (HashMap.)
        slice-idx (range slices)
        tier-gen  (fn [t]
                    (mapv (fn [n]
                            (->> slice-idx
                                 (filterv #(and (= (mod % t) n) (<= (+ % t) slices)))
                                 (mapv (fn [s]
                                         (let [node (newNode (range s (+ s t)))]
                                           (.put node-map (:key node) node)
                                           node)))))
                          (range t)))
        tiered-nodes (mapv #(tier-gen (inc %)) (range tiers))
        ,]
    (map->NodeDB {:LUT (into {} node-map) :TIERED tiered-nodes})))

(def nodeDB (newDB 16 4))


;; (map (fn [n] {:key (:key n) :slices (:slices n)}) (get-in nodes [0 0]))

;; (map (fn [n] {:key (:key n) :slices (:slices n)}) (get-in nodes [1 1]))


;; (map (fn [n] {:key (:key n) :slices (:slices n)}) (get-in nodes [2 2]))


(defn setupConnection [db]
  (let [tiered-nodes (apply concat (:TIERED db))]
    (loop [todo (rest  tiered-nodes)
           done (list (first tiered-nodes))]
      (if-let [curr-tier (first todo)]
        (let [curr-iter (clojure.lang.RT/iter curr-tier)
              prev-iter (clojure.lang.RT/iter (first done))]
          (loop [state :left-parent
                 curr-node   (.next curr-tier)
                 parent-node (.next prev-iter)]
            (case state
              :head
              :left-parent
              (if (connect-upstream curr-node parent-node)
                (if-let [next-parent (.next prev-iter)]
                  (recur :right-parent curr-node next-parent)
                  (recur :tail curr-node parent-node))
                (if-let [next-parent (.next prev-iter)]
                  (recur :left-parent curr-node next-parent)))
              :right-parent
              :tail))))
      )))

(defn setupConnection [db]
  (let [tiered-nodes (:TIERED db)]
    (loop [ ;; prev-tier-unused (first tiered-nodes)
           groups (rest tiered-nodes)
           group-idx 1]
      (when-let [curr-group (first groups)]
        (loop [tiers curr-group
               tier-idx 0]
          (when-let [tier (first tiers)]
            (let [curr-iter (clojure.lang.RT/iter tier)
                  prev-iter (if (= tier-idx 0)
                              (clojure.lang.RT/iter ((tiered-nodes (dec group-idx)) (dec group-idx)))
                              (clojure.lang.RT/iter ((tiered-nodes group-idx) (dec tier-idx))))]
              (when (= tier-idx 0)
                (loop [i (int (- (count (tiered-nodes (dec group-idx))) 2))]
                  (when (>= i 0)
                    (connect-upstream this upstream)
                    (recur (unchecked-dec-int i)))))
              (loop [curr-node   (.next curr-iter)
                     parent-node (.next prev-tier)
                     state  :phase1]
                (case state
                  :phase1 (if (connect-upstream curr-node parent-node)
                            (if (.hasNext prev-tier)
                              (recur curr-node (.next prev-tier) :phase2)
                              (do
                                (recur curr-node (.next prev-tier) :phase3))))
                  :phase2 (if (connect-upstream curr-node parent-node)
                            (if (.hasNext curr-tier)
                              (recur (.next curr-tier) parent-node :phase1))
                            (recur :phase3))
                  :phase3 ())
                ))


            (= tier-idx 0)

            :else
            (loop [tier tier
                   node-idx 0]
              (when-let [node (first tier)]
                (tap> {(:key node) [group-idx tier-idx node-idx]})
                (recur (rest tier) (inc node-idx))))
            (tap> "----------------")
            (recur (rest tiers) (inc tier-idx))))
        (tap> "=================")
        (recur (rest groups) (inc group-idx))))
    ))

(defn subSlices [slices & {:keys [sub-size margin] :or {sub-size (dec (count slices))
                                                        margin 1}}]
  (let [n     (count slices)
        start (- (first slices) margin)
        end   (- (+ (slices (dec n)) margin) sub-size)
        last-elem (slices (dec n))]
    (transduce (comp (map (fn [s]
                            (let [e (+ s sub-size)]
                              (when (<= e (+ last-elem margin))
                                (vec (range s e))))))
                     (filter #(not (nil? %))))
               conj
               (range start end))))



(transduce (comp (map (fn)))
           conj
           []
           [{:a 1 :b 2 :c 3} {:d 5 :e 6} {:f 7}])
