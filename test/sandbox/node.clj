(ns sandbox.node
  (:use clojure.core #_pct.common
        com.rpl.specter)
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector])
  (:import [java.util HashMap TreeSet TreeMap LinkedList Iterator]
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

(defprotocol IAsyncConnection
  (get-overlap [upstream downstream]            "given an downstream node, find the first continuous overlapping slices")
  (connect-upstream [this upstream]             "set up a connection with an upstream node")
  (connect-downstream* [this downstream slices] "** Should only be called from connect-upstream **")
  (disconnect-upstream [this upstream]          "disconnect with a upstream node")
  (disconnect-downstream* [this downstream]     "** Should only becalled from disconnect-upstream  **")
  (disconnect-all-upstreams [this]              "disconnect from all the upstream nodes")
  (upstream-full? [this]                        "test if upstream connection is saturated")
  (downstream-full? [this]                      "test if downstream connection is saturated")
  (get-connection-status [this]                 "get current upstream & downstream connection info as a persistent data"))

(spec/def ::async-node? (spec/keys :req-un [::data-in ::cmd-in ::__out ::data-out
                                            ::key ::slices
                                            ::mutex
                                            ::used ::unused
                                            ::upstream ::downstream
                                            ::iter ::state
                                            ::property]))


;; (deftype NodesTracking [used unused upstream downstream ^Lock mutex])


(defprotocol IConnection
  (add-node    [this node-key slices])
  (get-slices  [this] [this node])
  (get-nodes   [this])
  (remove-node [this node])
  (node-count  [this])
  (slice-count [this]))

(defrecord NodeConnection [^TreeSet connected ^HashMap connection]
  IConnection
  (add-node [this node-key slices]
    (.addAll connected slices)
    (.put connection node-key slices))

  (get-slices [this]
    (let [s ^TreeSet (TreeSet.)]
      (doseq [[k v] connection]
        (.addAll s v))
      ;; (into (sorted-set) s)
      s))

  (get-slices [this node-key]
    (.get connection node-key))

  (get-nodes [this]
    (keys connection))

  (remove-node [this node-key]
    (when-let [s (.get connection node-key)]
      (.remove connection node-key)
      (.removeAll connected s)))

  (node-count  [this] (.size connection))
  (slice-count [this] (.size connected)))

(defn newNodeConnection []
  (NodeConnection. (TreeSet.) (HashMap.)))

(defrecord ScrapNode [key slices
                      ^TreeSet unused
                      ^NodeConnection upstream ^NodeConnection downstream]
  IAsyncConnection
  (get-overlap [this downstream-node]
    (let [it     (clojure.lang.RT/iter unused)
          slices (:slices downstream-node)]
      (loop [usage (sorted-set)
             state :SEARCH
             prev-match (long 0)]
        (case state
          :SEARCH  (if (.hasNext it)
                     (let [s (long (.next it))]
                       (if (slices s)
                         (recur (conj usage s) :FOUND s)
                         (recur usage :SEARCH prev-match)))
                     usage)
          :FOUND (if (.hasNext it)
                   (let [s (long (.next it))]
                     (if (and (slices s) (= (- s prev-match) 1)) ;; making sure next slice is a continuous from previous one
                       (recur (conj usage s) :FOUND s)
                       usage))
                   usage)))))

  (connect-upstream [this upstream-node]
    (let [usage (get-overlap upstream-node this)]
      (if-not (empty? usage)
        (do ;; (.removeAll ^TreeSet (:unused upstream-node) usage)
          ;; (add-node (:downstream upstream-node) key usage)
          (add-node upstream (:key upstream-node) usage)
          (connect-downstream* upstream-node key usage)
          true)
        false)))

  (connect-downstream* [this downstream-key shared-slices]
    (add-node downstream downstream-key shared-slices)
    (.removeAll unused shared-slices))

  (disconnect-upstream [this upstream-node]
    (if-let [usage (get-slices upstream (:key upstream-node))]
      (do (remove-node upstream (:key upstream-node))
          (.addAll unused usage)
          (disconnect-downstream* upstream-node key)
          ;; (remove-node (:downstream upstream-node) key)
          ;; (.addAll ^TreeSet (:unused upstream-node) usage)
          )))

  (disconnect-downstream* [this downstream-key]
    (when-let [s (get-slices downstream downstream-key)]
      (.addAll unused s)
      (remove-node downstream downstream-key)))

  (disconnect-all-upstreams [this]
    (doseq [ups (get-nodes upstream)]
      (disconnect-upstream this ups)))

  (upstream-full? [this]
    (= (count slices) (slice-count upstream)))

  (downstream-full? [this]
    (= (count slices) (slice-count downstream))))

#_(defrecord AsyncNode [data-in cmd-in __out data-out  ;; commication
                      key slices
                      mutex                      ;; For IAsyncConnection
                      unused upstream downstream ;; tracking slice usage, connection
                      iter state ;; work thread related
                      properties ;; reconnstruction properties, e.g. each node could have different lambda parameter
                      ,]
  IAsyncConnection
  (get-overlap [this downstream-node]
    (let [it     (clojure.lang.RT/iter unused)
          slices (:slices downstream-node)]
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
    ;; lock both nodes before making any changes
    (.lock ^Lock mutex)
    (.lock ^Lock (:mutex upstream-node))
    (let [usage (get-overlap upstream-node this)]
      (if-not (empty? usage)
        (let [idx (indexOf (:slices upstream-node) (first usage))]
          (.removeAll ^TreeSet (:unused     upstream-node) usage)
          (.put       ^HashMap (:downstream upstream-node) key usage)
          (a/tap (:data-out upstream-node) data-in)
          (.put ^HashMap upstream (:key upstream-node) [usage [idx (count usage)]])
          (.unlock ^Lock mutex)
          (.unlock ^Lock (:mutex upstream-node))
          true)
        (do (.unlock ^Lock mutex)
            (.unlock ^Lock (:mutex upstream-node))
            false)))
    #_(let [usage (get-overlap upstream-node this) #_(set/intersection slices @(:unused other-node))]
      (if-not (empty? usage)
        (let [idx   (indexOf (:slices upstream-node) (first usage))]
          (swap! (:unused     upstream-node) #(set/difference % usage))
          (swap! (:downstream upstream-node) assoc key usage)
          (a/tap (:data-out   upstream-node) data-in)
          (swap! upstream assoc (:key upstream-node) [usage [idx (count usage)]])
          (.unlock ^Lock mutex)
          (.unlock ^Lock (:mutex upstream-node))
          true)
        (do (.unlock ^Lock mutex)
            (.unlock ^Lock (:mutex upstream-node))
            false))))
  (disconnect-upstream [this upstream-node]
    ;; lock both nodes before making any changes
    (.lock ^Lock mutex)
    (.lock ^Lock (:mutex upstream-node))
    (if-let [usage (.get ^HashMap upstream (:key upstream-node))]
      (do (.retainAll ^TreeSet (:unused     upstream-node) usage)
          (.remove    ^HashMap (:downstream upstream-node) key)
          (a/untap (:data-out upstream-node) data-in)
          (.remove    ^HashMap upstream (:key upstream-node))
          (.unlock ^Lock mutex)
          (.unlock ^Lock (:mutex upstream-node))
          true)
      (do (.unlock ^Lock mutex)
          (.unlock ^Lock (:mutex upstream-node))
          false))
    #_(swap! upstream (fn [ups]
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
            (a/untap data-out (:data-in other-node))))))
  (disconnect-all [this] nil)
  (full? [this]
    (.isEmpty ^java.util.Collection unused))
  (get-connection-status [this]
    (.lock ^Lock mutex)
    (let [status {:unused     (into (sorted-set) unused)
                  :downstream (into {} downstream)
                  :upstream   (into {} upstream)}]
      (.unlock ^Lock mutex)
      status)))


(defrecord NodeState [status parents])

(defrecord NodeDB [slices max-block LUT TIERED])

(defn newNode [slices]
  {:pre [(spec/valid? ::continuous-slice-ids? slices)]
   :post []}
  (let [out (a/chan 2)]
    #_(map->AsyncNode {:data-in  (a/chan 2)
                     :cmd-in   (a/chan 2)
                     :__out    out
                     :data-out (a/mult out)
                     :key      (keyword (clojure.string/join "-" slices))
                     :slices   (into (sorted-set) slices)
                     ;; :used     (atom {})
                     :mutex      (mutex/mutex)
                     :unused     (TreeSet. ^java.util.Collection slices)
                     :upstream   (HashMap.)
                     :downstream (HashMap.)
                     :properties (atom {}) ;; for potential parameter update
                     ;; also, potentially we could leave the node attached, and do multiple runs

                     ;; states
                     :iter     (atom (int 0))
                       :state    (atom (map->NodeState {:status :init :parents []}))})
    (map->ScrapNode {:key        (keyword (clojure.string/join "-" slices))
                    :slices     (into (sorted-set) slices)
                    :unused     (TreeSet. ^java.util.Collection slices)
                    :upstream   (newNodeConnection)
                    :downstream (newNodeConnection)})))

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
  (let [node-map ^HashMap (HashMap. ^int (int (* slices tiers))) ;; good estimate on size
        slice-idx (range slices)
        tier-gen  (fn [s]
                    (->> (range s)
                         (mapv (fn [n]
                                 (->> slice-idx
                                      (filterv #(and (= (mod % s) n) (<= (+ % s) slices)))
                                      ;; ((fn [x] (println (mapv #(range % (+ % s)) x)) ", " x))
                                      (mapv #(let [node (newNode (range % (+ % s)))]
                                               (.put node-map (:key node) node)
                                               node)))))
                         (filterv #(not (empty? %)))))
        tiered-nodes (mapv #(tier-gen %) (range 1 (inc tiers)))]
    (map->NodeDB {:slices slices :max-block tiers
                  :LUT (into {} node-map) :TIERED tiered-nodes})))

;; (def nodeDB (newDB 16 4))
;; (def nodeDB (newDB 5 4))
;; (map (fn [n] {:key (:key n) :slices (:slices n)}) (get-in nodes [0 0]))

;; (map (fn [n] {:key (:key n) :slices (:slices n)}) (get-in nodes [1 1]))


;; (map (fn [n] {:key (:key n) :slices (:slices n)}) (get-in nodes [2 2]))


(defn setupConnection [db]
  (let [tiered-nodes (apply concat (:TIERED db))]
    (loop [todo    (next  tiered-nodes)
           visited (list (first tiered-nodes))]
      (if todo
        (if-let [curr-tier (first todo)]
          (let [curr-it   ^Iterator (clojure.lang.RT/iter curr-tier)
                parent-it ^Iterator (clojure.lang.RT/iter (first visited))]
            (loop [state :head
                   curr-node   ^ScrapNode (.next curr-it)
                   parent-node ^ScrapNode (.next parent-it)]
              ;; (tap> [state (:slices parent-node) (:slices curr-node)])
              (case state
                :head
                (if (upstream-full? curr-node)
                  (if (.hasNext curr-it)
                    (recur :body (.next curr-it) parent-node))
                  (if (connect-upstream curr-node parent-node)
                    (do
                      (when-let [hist (next visited)]
                        (loop [p (first (first hist))
                               q (next hist)]
                          (if (connect-upstream curr-node p)
                            (if q
                              (recur (first (first q)) (next q))))))
                      (if (upstream-full? curr-node)
                        (if (.hasNext curr-it)
                          (recur :body (.next curr-it) parent-node))
                        (if (.hasNext parent-it)
                          (recur :body curr-node (.next parent-it))
                          (recur :tail-patch   curr-node parent-node))))
                    (do
                      (tap> curr-node)
                      (tap> parent-node)
                      (throw (Exception. "In setupConnection: :head, curr and parent node don't match")))))

                ;; :head-patch
                ;; (let [hist (next visited)]
                ;;   (loop [p (first (first hist))
                ;;          q (next hist)]
                ;;     (if (connect-upstream curr-node p)
                ;;       (if-not (upstream-full? curr-node)
                ;;         (if q
                ;;           (recur (first (first q)) (next q))))))
                ;;   (if (upstream-full? curr-node)
                ;;     (if (.hasNext curr-it)
                ;;       (recur :left-parent (.next curr-it) parent-node))
                ;;     (recur :tail-patch curr-node parent-node)))

                :body
                (if (upstream-full? curr-node)
                  (if (.hasNext curr-it)
                    (recur :body (.next curr-it) parent-node))
                  (if (connect-upstream curr-node parent-node)
                    (if (upstream-full? curr-node)
                      (if (.hasNext curr-it)
                        (recur :body (.next curr-it) parent-node))
                      (if (.hasNext parent-it)
                        (recur :body curr-node (.next parent-it))
                        (recur :tail-patch curr-node parent-node)))
                    (if (downstream-full? parent-node)
                      (if (.hasNext parent-it)
                        (recur :body curr-node (.next parent-it))
                        (recur :tail-patch curr-node parent-node))
                      (throw (Exception. "In setupConnection: both curr-node and parent-node not full and cannot connect.")))))

                ;; :left-parent
                ;; (if (connect-upstream curr-node parent-node)
                ;;   (if (.hasNext parent-it)
                ;;     (recur :right-parent curr-node (.next parent-it))
                ;;     (recur :tail-patch curr-node parent-node))
                ;;   (if (.hasNext parent-it)
                ;;     (recur state curr-node (.next parent-it))
                ;;     (recur :tail-patch curr-node parent-node)))

                ;; :right-parent
                ;; (if (connect-upstream curr-node parent-node)
                ;;   (if (upstream-full? curr-node)
                ;;     (if (.hasNext curr-it)
                ;;       (recur :left-parent (.next curr-it) parent-node))
                ;;     (throw (Exception. "In setupConnection: upstream is still not full after :right-parent connection")))
                ;;   (throw (Exception. "In setupConnection: :right-parent does not connect")))

                :tail-patch
                (let [hist (next visited)]
                  (loop [p (let [x (first hist)
                                 n (dec (count x))]
                             (x n))
                         q (next hist)]
                    (if (connect-upstream curr-node p)
                      (if-not (upstream-full? curr-node)
                        (if q
                          (let [x (first q)
                                n (dec (count x))]
                            (recur (x n) (next q)))))
                      (if q
                        (let [x (first q)
                              n (dec (count x))]
                          (recur (x n) (next q))))
                     ;; (do (throw (Exception. "In setupConnection: :tail-patch, ???")))
                      )
                   ;; (throw (Exception. "Premature finish"))
                    ))))
            (recur (next todo) (conj visited curr-tier))))

        ;; wrapping around
        (let []
          ;; (tap> (transform [ALL ALL] (fn [x] {(:slices x) (:unused x)}) visited))
          (loop [hist visited
                 idle-slices (:slices db)]
            (if-let [block (first hist)]
              (when (> idle-slices 0)
                (let [n (loop [nodes block
                               n 0]
                          (if-let [x (first nodes)]
                            (let [unused (:unused x)]
                              (if (empty? unused)
                                (recur (next nodes) n)
                                (let [n (+ n (count unused))]
                                  (doseq [i unused]
                                    (let [k (keyword (str i))
                                          d (k (:LUT db))]
                                      (connect-upstream d x)))
                                  (recur (next nodes) n))))
                            n))]
                  (recur (next hist) (- idle-slices n)))))))))))

(def grid-walker (recursive-path [] p (if-path vector? [ALL p] STAY)))
(spec/def ::all-slices-used?
  (fn [db]
    (->> (:TIERED db)
         (select grid-walker)
         (transform [ALL] (fn [x] (and (empty? (:unused x))
                                      (= (:slices x) (.connected  ^NodeConnection (:upstream x)))
                                      (= (:slices x) (.connected  ^NodeConnection (:downstream x))))))
         (every? true?))))



(def nodeDB (newDB 5 4))
(setupConnection nodeDB)
(spec/valid? ::all-slices-used? nodeDB)
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:upstream x))})  (:TIERED nodeDB)))
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection(:downstream x))}) (:TIERED nodeDB)))

(def testDB (newDB 6 4))
(setupConnection testDB)
(spec/valid? ::all-slices-used? testDB)
(tap> (transform [ALL ALL ALL] :slices (:TIERED testDB)))
(tap> (transform [ALL ALL]     :slices (next (apply concat (:TIERED testDB)))))
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:upstream x))})  (:TIERED testDB)))
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection(:downstream x))}) (:TIERED testDB)))

(def testDB2 (newDB 7 5))
(setupConnection testDB2)
(spec/valid? ::all-slices-used? testDB2)
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:upstream x))})   (:TIERED testDB2)))
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:downstream x))}) (:TIERED testDB2)))


(def testDB3 (newDB 8 5))
(setupConnection testDB3)
(spec/valid? ::all-slices-used? testDB3)
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:upstream x))})   (:TIERED testDB3)))
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:downstream x))}) (:TIERED testDB3)))


(def testDB4 (newDB 17 5))
(setupConnection testDB4)
(spec/valid? ::all-slices-used? testDB4)
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:upstream x))})   (:TIERED testDB4)))
(tap> (transform [ALL ALL ALL] (fn [x] {(:slices x) (.connection ^NodeConnection (:downstream x))}) (:TIERED testDB4)))

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




(defrecord Partition [key slices])

(defn newPartition [slices]
  {:pre [(spec/valid? ::continuous-slice-ids? slices)]
   :post []}
  (map->Partition {:key (keyword (clojure.string/join "-" slices))
                   :slices (into (sorted-set) slices)}))

(defn setupPartitions [slices tiers]
  (let [partition-map ^HashMap (HashMap.)
        slice-idx (range slices)
        tier-gen  (fn [t]
                    (mapv (fn [n]
                            (->> slice-idx
                                 (filterv #(and (= (mod % t) n) (<= (+ % t) slices)))
                                 (mapv (fn [s]
                                         (let [part (newPartition (range s (+ s t)))]
                                           (.put partition-map (:key part) part)
                                           part)))))
                          (range t)))
        tiered-partitions (mapv #(tier-gen (inc %)) (range tiers))
        ,]
    {:LUT (into {} partition-map) :TIERED tiered-partitions}))


(def n1 (into (sorted-set) [1 2 3]))
(def n2 (into (sorted-set) [4 5 6]))
(def n3 (into (sorted-set) [2 3 4]))

(defn test []
  (setupPartition '(1 2 3 3))
  (indexOf ))
