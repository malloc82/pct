(ns pct.async.node
  (:use clojure.core #_pct.common
        com.rpl.specter)
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector])
  (:import [java.util HashMap TreeSet TreeMap LinkedList Iterator]
           [java.util.concurrent.locks Lock]
           [clojure.core.async.impl.channels ManyToManyChannel]))

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

#_(spec/def ::async-node? (spec/keys :req-un [::data-in ::cmd-in ::__out ::data-out
                                            ::key ::slices
                                            ::mutex
                                            ::used ::unused
                                            ::upstream ::downstream
                                            ::iter ::state
                                            ::property]))

(defprotocol IConnection
  (add-node    [this node-key slices])
  (get-slices  [this] [this node])
  (get-nodes   [this])
  (remove-node [this node])
  (node-count  [this])
  (slice-count [this]))


(defprotocol IAsyncNode
  (get-overlap              [upstream downstream]    "given an downstream node, find the first continuous overlapping slices")
  (connect-upstream         [this upstream]          "set up a connection with an upstream node")
  (connect-downstream*      [this downstream slices] "** Should only be called from connect-upstream **")
  (disconnect-upstream      [this upstream]          "disconnect with a upstream node")
  (disconnect-downstream*   [this downstream]        "** Should only becalled from disconnect-upstream  **")
  (disconnect-all-upstreams [this]                   "disconnect from all the upstream nodes")
  (upstream-full?           [this]                   "test if upstream connection is saturated")
  (downstream-full?         [this]                   "test if downstream connection is saturated")
  (get-connection-status    [this]                   "get current upstream & downstream connection info as a persistent data"))


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

(defn newNodeConnection
  ([]
   (NodeConnection. (TreeSet.) (HashMap.)))
  ([slice-count]
   (NodeConnection. (TreeSet.) (HashMap. ^int (int slice-count)))))


(defrecord AsyncNode [key slices
                      ^ManyToManyChannel ch-in ^ManyToManyChannel ch-out ch-mult
                      ^TreeSet unused
                      ^NodeConnection upstream ^NodeConnection downstream
                      ^HashMap local-offsets ^clojure.lang.PersistentVector global-offset]
  IAsyncNode
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
          (a/tap (:ch-mult upstream-node) ch-in)
          (connect-downstream* upstream-node key usage)
          true)
        false)))

  (connect-downstream* [this downstream-key shared-slices]
    (add-node downstream downstream-key shared-slices)
    (.removeAll unused shared-slices))

  (disconnect-upstream [this upstream-node]
    (if-let [usage (get-slices upstream (:key upstream-node))]
      (do (remove-node upstream (:key upstream-node))
          (a/untap (:ch-mult upstream-node) ch-in)
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


(defn newAsyncNode [slices]
  {:pre [(spec/valid? ::continuous-slice-ids? slices)]
   :post []}
  (let [ch-out (a/chan 4)
        n (int (count slices))]
    (map->AsyncNode {:ch-in      (a/chan 4)
                     :ch-out     ch-out
                     :ch-mult    (a/mult ch-out)
                     :key        (keyword (clojure.string/join "-" slices))
                     :slices     (into (sorted-set) slices)
                     :unused     (TreeSet. ^java.util.Collection slices)
                     :upstream   (newNodeConnection n)
                     :downstream (newNodeConnection n)
                     :global-offset [(int (first slices)) n (int 0)] ;; offset-x, length, offset-y
                     :local-offsets (HashMap. n)})))

(defprotocol IAsyncGrid
  (connect-nodes [this])
  (compute-offsets [this]))

(defrecord AsyncGrid [^int slice-count ^int max-block-size
                      ^clojure.lang.PersistentHashMap lut ^clojure.lang.PersistentVector grid]
  IAsyncGrid
  (connect-nodes [this]
    (let [tiered-nodes (apply concat grid)]
      (loop [todo      (next  tiered-nodes)
             visited   (list (first tiered-nodes))]
        (if todo
          (if-let [curr-tier (first todo)]
            (let [curr-it   ^Iterator (clojure.lang.RT/iter curr-tier)
                  parent-it ^Iterator (clojure.lang.RT/iter (first visited))]
              (loop [state :head
                     curr-node   ^AsyncNode (.next curr-it)
                     parent-node ^AsyncNode (.next parent-it)]
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
                   idle-slices slice-count]
              (if-let [block (first hist)]
                (when (> idle-slices 0)
                  (let [n (int (loop [nodes block
                                      n 0]
                                 (if-let [x (first nodes)]
                                   (let [unused (:unused x)]
                                     (if (empty? unused)
                                       (recur (next nodes) n)
                                       (let [n (+ n (count unused))]
                                         (doseq [i unused]
                                           (let [k (keyword (str i))
                                                 d (k lut)]
                                             (connect-upstream d x)))
                                         (recur (next nodes) n))))
                                   n)))]
                    (recur (next hist) (- idle-slices n)))))))))))

  (compute-offsets [this]
    (doseq [[_ node] lut]
      (let [ups ^HashMap  (.connection ^NodeConnection (:upstream node))
            local-head    (first (:slices node))
            local-offsets ^HashMap (:local-offsets node)]
        (doseq [[k v] ups]
          ;; (tap> ["here" k v])
          (let [seg-head (first v)
                up-head  (-> (k lut) :slices first)]
            ;; (tap> ["here2:" seg-head up-head (k lut) (-> (k lug) :slices)])

            ;; used for copying data from reveived vector to local vector:
            ;;
            ;; (copy! x y offset-x length offset-y)  ;; copy from x to y
            ;; x: received vector
            ;; y: local vector
            ;; offset-x: offset on receved vector
            ;; offset-y: offset on local vector
            ;;
            ;; we are basically calculating [offset-rec length offset-loc]
            ;; this way we can just call:
            ;; (apply copy! x y (rest v))
            #_(.put ups k [v
                           (- seg-head up-head)
                           (count v)
                           (- seg-head local-head)])
            (.put local-offsets k  [(- seg-head up-head) (count v) (- seg-head local-head)])))))))

(defn newAsyncGrid [slice-count max-block-size]
  (let [node-map ^HashMap (HashMap. ^int (int (* slice-count max-block-size))) ;; good estimate on size
        slice-idx (range slice-count)
        block-gen  (fn [block-size]
                    (->> (range block-size)
                         (mapv (fn [bi]
                                 (->> slice-idx
                                      ;; get the starting index of each block
                                      (filterv #(and (= (mod % block-size) bi) (<= (+ % block-size) slice-count)))
                                      ;; ((fn [x] (println (mapv #(range % (+ % s)) x)) ", " x))
                                      (mapv #(let [node (newAsyncNode (range % (+ % block-size)))]
                                               (.put node-map (:key node) node)
                                               node)))))
                         (filterv #(not (empty? %)))))
        blocks (mapv #(block-gen %) (range 1 (inc max-block-size)))]
    (map->AsyncGrid {:slice-count (int slice-count) :max-block-size (int max-block-size)
                     :lut (into {} node-map) :grid blocks})))


(def grid-walker (recursive-path [] p (if-path vector? [ALL p] STAY)))
(spec/def ::all-slices-used?
  (fn [grid]
    (->> (:grid grid)
         (select grid-walker)
         (transform [ALL] (fn [x] (and (empty? (:unused x))
                                      (= (:slices x) (.connected  ^NodeConnection (:upstream x)))
                                      (= (:slices x) (.connected  ^NodeConnection (:downstream x))))))
         (every? true?))))

