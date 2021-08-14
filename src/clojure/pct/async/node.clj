(ns pct.async.node
  (:use clojure.core #_pct.common
        com.rpl.specter)
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async.impl.mutex :as mutex]
            [clojure.inspector :as inspector]
            [taoensso.timbre :as timbre])
  (:import [java.util HashMap TreeSet TreeMap LinkedList Iterator Arrays]
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

(spec/def ::int-seq? (spec/coll-of (spec/and int? pos?)))
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


(defn to-key
  "transform a given sequence to key"
  ^clojure.lang.Keyword [s]
  (keyword (clojure.string/join "-" s)))

(defprotocol IConnection
  (add-node    [this node-key slices])
  (get-slices  [this] [this node])
  (get-nodes   [this])
  (remove-node [this node])
  (node-count  [this])
  (slice-count [this]))


(defprotocol IAsyncSetup
  (get-overlap              [upstream downstream]    "given an downstream node, find the first continuous overlapping slices")
  (connect-upstream         [this upstream]          "set up a connection with an upstream node")
  (connect-downstream*      [this downstream slices] "** Should only be called from connect-upstream **")
  (disconnect-upstream      [this upstream]          "disconnect with a upstream node")
  (disconnect-downstream*   [this downstream]        "** Should only becalled from disconnect-upstream  **")
  (disconnect-all-upstreams [this]                   "disconnect from all the upstream nodes")
  (upstream-full?           [this]                   "test if upstream connection is saturated")
  (downstream-full?         [this]                   "test if downstream connection is saturated")
  (get-connection-status    [this]                   "get current upstream & downstream connection info as a persistent data")

  (send-downstream          [this data]              "send data downstream")
  (send-upstream            [this data]              "send data upstream")
  (recv-upstream            [this x] [this x term]))

(defprotocol IAsyncCommunication
  (asyncTest [this f] "For testing, f args: [in out]"))

(defprotocol IAsyncCompute)

(defprotocol INodeInfo
  (setProperty   [this k v])
  (updateProperty [this k f] [this k f a] [this k f a b])
  (getProperty   [this k])
  (getProperties [this])
  (clearProperties! [this]))

(defrecord NodeConnection [^TreeSet connected ^HashMap connection ^HashMap hidden]
  IConnection
  (add-node [this node-key chan+slices]
    (let [[_ slices] chan+slices]
      (.addAll connected slices)
      (.put connection node-key chan+slices)))

  (get-slices [this]
    (let [s ^TreeSet (TreeSet.)]
      (doseq [[k [c v]] connection]
        (.addAll s v))
      ;; (into (sorted-set) s)
      s))

  (get-slices [this node-key]
    ((.get connection node-key) 1))

  (get-nodes [this]
    (keys connection))

  (remove-node [this node-key]
    (when-let [[_ s] (.get connection node-key)]
      (.remove connection node-key)
      (.removeAll connected s)))

  (node-count  [this] (.size connection))
  (slice-count [this] (.size connected)))

(defn newNodeConnection
  ([]
   (NodeConnection. (TreeSet.) (HashMap.) (HashMap.)))
  ([slice-count]
   (NodeConnection. (TreeSet.) (HashMap. ^int (int slice-count)) (HashMap.))))


(defrecord AsyncNode [key slices ^long offset
                      ^ManyToManyChannel ch-in ^ManyToManyChannel ch-out
                      ^ManyToManyChannel ch-log ;; data logging, for debugging & testing
                      ^TreeSet unused
                      ^NodeConnection upstream ^NodeConnection downstream
                      ^clojure.lang.PersistentVector global-offset
                      properties]
  clojure.lang.IFn
  (invoke [this] (count slices))
  (invoke [this k] (k this))

  IAsyncSetup
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
      (if ((:key upstream-node) (:connection upstream))
        true
        (if-let [s (first usage)]
          (let [offset-ups   (- s (first (:slices upstream-node)))
                offset-local (- s (first slices))]
            ;; (.removeAll ^TreeSet (:unused upstream-node) usage)
            ;; (add-node (:downstream upstream-node) key usage)
            (add-node upstream (:key upstream-node) [[(* offset offset-ups) (* offset (count usage)) (* offset offset-local)] usage])
            ;; (a/tap (:mux-out upstream-node) ch-in)
            ;; (connect-downstream* upstream-node key usage)
            (connect-downstream* upstream-node this usage)
            true)
          false))))


  (connect-downstream* [this downstream-node shared-slices]
    (add-node downstream (:key downstream-node) [(:ch-in downstream-node) shared-slices])
    (.removeAll unused shared-slices))

  (disconnect-upstream [this upstream-node]
    (if-let [usage (get-slices upstream (:key upstream-node))]
      (do (remove-node upstream (:key upstream-node))
          ;; (a/untap (:mux-out upstream-node) ch-in)
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
    (= (count slices) (slice-count downstream)))


  (send-downstream [this data]
    (let [^HashMap ds (.connection downstream)]
      (doseq [[k [c _]] ds]
        ;; (println "Sending data to " k)
        (a/put! c data))
      true))


  (recv-upstream [this x]
    (let [^HashMap connection (.connection upstream)]
     (loop [remaining (into {} connection)]
       (if (empty? remaining)
         (do x)
         (if-let [[^clojure.lang.Keyword k v] (a/<!! ch-in)]
           (if-let [[[^long offset-v ^long length ^long offset-local]] (.get connection k)]
             (do #_(timbre/info (format "%s%s, (%d) : received data from %s" _normal_ thread-name iter k))
                 (System/arraycopy ^doubles v offset-v ^doubles x offset-local length)
                 (recur (dissoc remaining k)))
             (do (timbre/info (format "%s : could not find key %s, skip." key k))
                 (recur remaining)))
           (do (timbre/info (format "%s : input channel closed." key))
               nil))))))


  ;; (recv-upstream [this x slice-offset]
  ;;   (loop [remaining (into {} local-offsets)]
  ;;     (if (empty? remaining)
  ;;       (do x)
  ;;       (if-let [[^clojure.lang.Keyword k v] (a/<!! ch-in)]
  ;;         (if-let [[^long offset-v ^long length ^long offset-local] (.get local-offsets k)]
  ;;           (do #_(timbre/info (format "%s%s, (%d) : received data from %s" _normal_ thread-name iter k))
  ;;               (System/arraycopy ^doubles v (* offset-v     slice-offset)
  ;;                                 ^doubles x (* offset-local slice-offset)
  ;;                                 (* length slice-offset))
  ;;               (recur (dissoc remaining k)))
  ;;           (do (timbre/info (format "%s : could not find key %s, skip." key k))
  ;;               (recur remaining)))
  ;;         (do (timbre/info (format "%s : input channel closed." key))
  ;;             nil)))))

  (recv-upstream [this x term]
    (let [^HashMap connection (.connection upstream)]
     (loop [remaining (into {} connection), valid true]
       (if (empty? remaining)
         (do (if valid x nil))
         (if-let [[^clojure.lang.Keyword k v] (a/<!! ch-in)]
           (if (= k term)
             (recur (dissoc remaining v) false)
             (if-let [[[^long offset-v ^long length ^long offset-local]] (.get connection k)]
               (do #_(timbre/info (format "%s%s, (%d) : received data from %s" _normal_ thread-name iter k))
                   (System/arraycopy ^doubles v offset-v ^doubles x offset-local length)
                   (recur (dissoc remaining k) valid))
               (do (timbre/info (format "%s : could not find key %s, skip." key k))
                   (recur remaining valid))))
           (do (timbre/info (format "%s : input channel closed." key))
               nil))))))


  ;; IAsyncCommunication
  ;; (distribute [this f]
  ;;   (f this))

  INodeInfo
  (setProperty [this k v]    (swap! properties assoc k v))
  (updateProperty [this k f] (swap! properties update k f))
  (getProperty [this k]    (get @properties k))
  (getProperties [this]    @properties)
  (clearProperties! [this] (reset! properties {}))

  java.io.Closeable
  (close [this]
    (a/close! ch-in)
    (a/close! ch-out)
    (a/close! ch-log)))


(defn newAsyncNode
  ([slices ^long slice-offset]
   (newAsyncNode slices slice-offset :body))
  ([slices ^long slice-offset type]
   {:pre [(spec/valid? ::continuous-slice-ids? slices)
          (spec/valid? #{:head :body :fake} type)]
    :post []}
   (let [;; ch-out (a/chan 4)
         n (int (count slices))]
     (map->AsyncNode {:type       type
                      :ch-in      (a/chan 4)
                      :ch-out     (a/chan 4)
                      ;; :mux-out    (a/mult ch-out)
                      :ch-log     (a/chan 4)
                      :key        (keyword (clojure.string/join "-" slices))
                      :slices     (into (sorted-set) slices)
                      :offset     (long slice-offset)
                      :unused     (TreeSet. ^java.util.Collection slices)
                      :upstream   (newNodeConnection n)
                      :downstream (newNodeConnection n)
                      :global-offset [(int (first slices)) n (int 0)] ;; offset-x, length, offset-y
                      ;; :local-offsets (HashMap. n)
                      :properties (atom {})}))))


(defprotocol IAsyncGrid
  (get-heads [this])
  (get-grid  [this])
  (get-table [this])
  (connect-nodes   [this])
  (compute-offsets [this])

  (get-followers     [this head])
  (nodes-start-with  [this i]    "find a list of nodes that start with a particular slice")
  (clear-channels    [this])
  (trim-connections  [this]    "Remove direct connections from both upstream and downstream that have
                                the same starting slice, this is for optimized version of reconstruction")

  (distribute-selected [this cond-fn f] [this cond-fn f args] "apply f to selected nodes, filtered by cond-fn")
  (distribute-all      [this f] [this f args]                 "apply f to every node")
  (distribute-heads    [this f args]                          "distribute function through heads")
  (collect-heads       [this]                                 "collect data from heads")
  (collect-data    [this] [this cond-fn]      "Collect one data from each of the selected node, filtered by cond-fn")
  (data-log   [this]  "get a data log channel"))

(def grid-walker (recursive-path [] p (if-path vector? [ALL p] STAY)))

(deftype AsyncGrid [^int slice-count
                    ^clojure.lang.PersistentVector  block-sizes ;; should be sorted from small to large
                    ^clojure.lang.PersistentHashSet head-nodes
                    ^clojure.lang.PersistentHashMap lut-heads
                    ^clojure.lang.PersistentHashMap lut
                    ^clojure.lang.PersistentVector  grid]
  clojure.lang.Counted
  (count [this] (count lut))

  ;; java.util.Map
  ;; (clear [thisf])
  ;; (containsKey [this k])
  ;; (containsValue [this val])
  ;; (entrySet [this])
  ;; (get [this k]
  ;;   (lut k))
  ;; (hashCode [this])
  ;; (isEmpty [this])
  ;; (keySet [this])
  ;; (put [this k v])
  ;; (putAll [this m])
  ;; (remove [this k])
  ;; (size [this])
  ;; (values [this])

  clojure.lang.IFn
  (invoke [this] (count lut))
  (invoke [this k] (lut k))
  (invoke [this i j] ((grid i) j))
  (invoke [this i j k] (((grid i) j) k))
  (applyTo [this arglist]
    (let [[a b c] arglist
          n (count arglist)]
      (case n
        0 (.invoke this)
        1 (.invoke this a)
        2 (.invoke this a b)
        3 (.invoke this a b c)
        (throw (Exception. (format "AsyncGrid.applyTo: invalid arglist, expected 1, 2 or 3, got %d." n))))))

  clojure.lang.ILookup
  (valAt [this k]           (get lut k nil) #_(.valAt this k nil))
  (valAt [this k not-found] (get lut k not-found))

  clojure.lang.IKeywordLookup
  (getLookupThunk [this k]
    (reify clojure.lang.ILookupThunk
      (get [thunk g]
        (if (identical? (class this) (class g))
          (lut k)
          thunk))))

  java.lang.Iterable
  (iterator [this]
    (clojure.lang.RT/iter (select grid-walker grid)))

  IAsyncGrid
  (get-heads [this] head-nodes)
  (get-grid  [this] grid)
  (get-table [this] lut)

  (connect-nodes [this]
    (let [tiered-nodes (apply concat grid)]
      (loop [todo      (next  tiered-nodes)
             visited   (list (first tiered-nodes))]
        (let [curr-tier (first todo)]
          (if (and todo (not (empty? curr-tier)))
            (let [curr-it   ^Iterator (clojure.lang.RT/iter curr-tier)
                  parent-it ^Iterator (clojure.lang.RT/iter (first visited))]
              ;; (tap> ["loop:" todo curr-tier (:key (first curr-tier)) (:key (first (first visited)))])
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
                        #_(when-let [hist (next visited)]
                            (loop [p (first (first hist))
                                   q (next hist)]
                              (if (connect-upstream curr-node p)
                                (if q
                                  (recur (first (first q)) (next q))))))
                        (loop [hist (next visited)]
                          (if hist
                            (let [p ((first hist) 0)]
                              (if (connect-upstream curr-node p)
                                (recur (next hist))))))
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
                  #_(if (upstream-full? curr-node)
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
                  (do (connect-upstream curr-node parent-node)
                      (cond
                        (upstream-full? curr-node)
                        (if (.hasNext curr-it)
                          (recur :body (.next curr-it) parent-node))

                        (downstream-full? parent-node)
                        (if (.hasNext parent-it)
                          (recur :body curr-node (.next parent-it))
                          (recur :tail-patch curr-node parent-node))

                        :else
                        (do
                          (tap> curr-node)
                          (tap> parent-node)
                          (throw (Exception. "In setupConnection: :body, neither curr and parent node are full after connection attempt.")))))

                  :tail-patch
                  (loop [hist (next visited)]
                    (if hist
                      (let [x (first hist)
                            p (x (dec (count x)))]
                        (connect-upstream curr-node p)
                        (if-not (upstream-full? curr-node)
                          (recur (next hist))))))
                  #_(let [hist (next visited)]
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
              (recur (next todo) (conj visited curr-tier)))

            ;; wrapping around
            (let []
              ;; (tap> (transform [ALL ALL] (fn [x] {(:slices x) (:unused x)}) visited))
              (loop [hist visited
                     idle-slices (count head-nodes) ;; slice-count
                     ]
                (if-let [block (first hist)]
                  (when (> idle-slices 0)
                    (let [n (int (loop [nodes block, n 0]
                                   (if-let [x (first nodes)]
                                     (let [#_#_unused (:unused x)
                                           it (clojure.lang.RT/iter (vec (:unused x)))]
                                       (if (.hasNext it) #_(empty? unused)
                                           (recur (next nodes)
                                                  (long (loop [n (long n)]
                                                          (if (.hasNext it)
                                                            (let [k (keyword (str (.next it))),
                                                                  d (k lut-heads)]
                                                              (if (connect-upstream d x)
                                                                (recur (unchecked-inc n))
                                                                (recur n)))
                                                            n))))
                                           (recur (next nodes) n)
                                           #_(let [n (+ n (count unused))]
                                               (doseq [i unused]
                                                 (let [k (keyword (str i))
                                                       d (k lut-heads)]
                                                   (connect-upstream d x)))
                                               (recur (next nodes) n))))
                                     n)))]
                      (recur (next hist) (- idle-slices n))))))))))))

  (compute-offsets [this]
    (doseq [[_ node] lut]
      (let [ups ^HashMap  (.connection ^NodeConnection (:upstream node))
            local-head    (first (:slices node))
            local-offsets ^HashMap (:local-offsets node)]
        (doseq [[k [_ v]] ups]
          ;; (tap> ["here" k v])
          (let [seg-head ^long (first v)
                up-head  ^long (-> (k lut) :slices first)]
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
            (try
              (.put local-offsets k  [(- seg-head up-head) (count v) (- seg-head local-head)])
              (catch java.lang.NullPointerException e
                (println (:slices node) seg-head up-head k (k lut))
                (throw e))))))))


  (get-followers [this head-node]
    (let [i (first (:slices head-node))]
      (remove nil?
            (mapv (fn [s]
                    (let [k (to-key (range i (+ i s)))]
                      (k lut)))
                  block-sizes))))


  (nodes-start-with [this i]
    (remove nil?
            (mapv (fn [s]
                    (let [k (to-key (range i (+ i s)))]
                      (k lut)))
                  block-sizes)))


  (clear-channels [this]
    (doseq [[k v] lut]
      (send-downstream v :clear))
    (doseq [[k v] lut]
      (loop []
        (if-let [data (a/poll! (:ch-in v))]
          (if (not (= data :clear))
            (do (timbre/info (format "Async clear-channels: (%s) old data cleared" (:key v)))
                (recur)))
          (timbre/info (format "Async clear-channels: Something wrong, did not receive data from node %s" (:key v)))))))


  (trim-connections [this]
    (doseq [[_ v] lut]
      (let [^HashMap m      (-> v :downstream :connection)
            ^HashMap hidden (-> v :downstream :hidden)]
        (doseq [k (keys m)]
          (when (= (-> v :slices first) (-> (k lut) :slices first))
            (.put hidden k (.remove m k)))))
      (let [^HashMap m      (-> v :upstream :connection)
            ^HashMap hidden (-> v :upstream :hidden)]
        (doseq [k (keys m)]
          (when (= (-> v :slices first) (-> (k lut) :slices first))
            (.put hidden k (.remove m k))))))
    this)


  (distribute-selected [this cond-fn f]
    (loop [s (select [grid-walker cond-fn] grid)]
      (when-let [node (first s)]
        (f node)
        (recur (next s)))))

  (distribute-selected [this cond-fn f args]
    (loop [s (select [grid-walker cond-fn] grid)]
      (when-let [node (first s)]
        (apply f node args)
        (recur (next s)))))

  (distribute-all [this f]
    (loop [s (select grid-walker grid)]
      (when-let [node (first s)]
        ;; (tap> ["f => " (:key node)])
        (f node)
        (recur (next s)))))

  (distribute-all [this f args]
    (loop [s (select grid-walker grid)]
      (when-let [node (first s)]
        (apply f node args)
        (recur (next s)))))


  (distribute-heads [this f args]
    (doseq [h head-nodes]
      (apply f (get-followers this h) args)))


  (collect-heads [this])

  (collect-data [this] ;; collection data from head nodes
    (let [res-ch (a/chan (count head-nodes))]
      (a/go
        (loop [remaining (into #{} (mapv :ch-out head-nodes))]
          ;; (timbre/info (format "collect-data: remaining %s" remaining))
          (if (empty? remaining)
            (a/close! res-ch)
            (let [[[k, :as data] ch] (a/alts! (vec remaining))]
              (if (or (nil? k) (= k :end))
                (recur (disj remaining ch))
                (do (a/>! res-ch data)
                    (recur remaining)))))))
      res-ch))

  (collect-data [this cond-fn]
    (a/go
      (let [nodes (select [grid-walker cond-fn] grid)
            cs (mapv :ch-out nodes)]
        (loop [remaining (into #{} (mapv :key nodes))
               acc (transient {})]
          ;; (timbre/info (format "collect-data: remaining %s" remaining))
          (if (empty? remaining)
            (persistent! acc)
            (let [[[k data] ch] (a/alts! cs)]
              (if (and k (remaining k))
                (recur (disj remaining k) (assoc! acc k data))
                (recur remaining acc)))))))

    #_(a/thread
        (let [acc ^HashMap (HashMap.)]
          (loop [remaining (into #{} (mapv :ch-log (select [grid-walker cond-fn] grid)))]
            (if (empty? remaining)
              (a/>!! out acc)
              (let [[[k data] ch] (a/alts!! (vec remaining))]
                (if k
                  (.put acc k data))
                (recur (disj remaining ch))))))))

  (data-log [this]
    (let [log-ch (a/chan (* (count lut) 2))
          mix-out (a/mix log-ch)]
      (doseq [d (select grid-walker grid)]
        (a/admix mix-out (:ch-log d)))
      log-ch))

  java.io.Closeable
  (close [this]
    (doseq [[_ node] lut]
      (.close ^AsyncNode node))))

#_(defn newAsyncGrid
  ([slice-count max-block-size]
   (newAsyncGrid slice-count max-block-size nil))
  ([slice-count max-block-size connect?]
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
         blocks (mapv #(block-gen %) (range 1 (inc max-block-size)))
         grid   (AsyncGrid. (int slice-count) (int max-block-size) (into {} node-map) blocks)]
     (if connect?
       (doto grid
         (connect-nodes)
         (compute-offsets))
       grid)
     #_(map->AsyncGrid {:slice-count (int slice-count) :max-block-size (int max-block-size)
                        :lut (into {} node-map) :grid blocks}))))


(defn newAsyncGrid
  ([slice-count block-sizes & {:keys [connect? slice-offset] :or {slice-offset 1}}]
   {:pre [(spec/valid? ::int-seq? block-sizes)]}
   (let [sorted-block-sizes (sort block-sizes)
         node-map  ^HashMap (HashMap. ^int (reduce + (map (fn [n] (int (- slice-count (dec n)))) block-sizes))) ;; good estimate on size
         ;; slice-idx (range slice-count)
         head-size (first sorted-block-sizes)
         head-map ^HashMap (HashMap.)
         block-gen (fn [block-size]
                     (->> (range block-size)
                          (mapv (fn [start-idx] ;; start-idx -> vector of nodes
                                  (->> (if (and (= block-size head-size) (= start-idx 0))
                                         (partition-all block-size (range start-idx slice-count))
                                         (partition     block-size (range start-idx slice-count)))
                                       (mapv #(let [node (if (and (= block-size head-size)
                                                                  (= start-idx 0))
                                                           (if (= block-size (count %))
                                                             (newAsyncNode % slice-offset :head)
                                                             (newAsyncNode % slice-offset :fake))
                                                           (newAsyncNode % slice-offset :body))]
                                                (.put node-map (:key node) node)
                                                (let [type (:type node)]
                                                  (when (or (= type :head) (= type :fake))
                                                    (doseq [i (:slices node)]
                                                      (.put head-map (keyword (str i)) node))
                                                    (.put head-map (:key node) node)))
                                                node)))))
                          (filterv #(not (empty? %)))))
         blocks (mapv #(block-gen %) block-sizes)
         grid   (AsyncGrid. (int slice-count)
                            (vec sorted-block-sizes)
                            (into #{} (vals head-map))
                            (into {} head-map)
                            (into {} node-map)
                            blocks)]
     (if connect?
       (doto grid
         (connect-nodes)
         #_(compute-offsets))
       grid))))




(spec/def ::all-slices-used?
  (fn [x] (and (empty? (:unused x))
              (= (:slices x) (.connected  ^NodeConnection (:upstream x)))
              (= (:slices x) (.connected  ^NodeConnection (:downstream x))))))

(spec/def ::local-offsets-length-consistent?
  #(= (count (:slices %)) (reduce + (mapv (fn[[_ v]] (v 1)) (:local-offsets %)))))

(defn local-offset-range-check [x]
  (mapv (fn [[k v]]
          [k (if-let [s (clojure.string/split (str k) #"-")]
               (let [slen (count s)
                     n    (count (:slices x))]
                 ;; (tap> [v s slen n])
                 (and (< (v 0) slen) (<= (+ (v 0) (v 1)) slen)
                      (< (v 2) n)    (<= (+ (v 2) (v 1)) n)))
               false)])
        (:local-offsets x)))

(spec/def ::local-offsets-in-range? #(every? second
                                             (let [res (local-offset-range-check %)]
                                               ;; (tap> res)
                                               res)))


(defn local-offsets-check [x]
  (let [idx-list (sort-by first
                          (mapv (fn [[k v]]
                                  [(v 2) (v 1)])
                                (:local-offsets x)))]
    (loop [idx-list idx-list
           idx (long 0)]
      (if-let [[^long i ^long len] (first idx-list)]
        (if (= idx i)
          (recur (next idx-list) (+ i len))
          false)
        (= idx (count (:slices x)))))))

(spec/def ::local-offsets-consistent? #(local-offsets-check %))

(defn global-offsets-check [s]
  (let [[g-offset length l-offset] (:global-offset s)]
    (and (= g-offset (first (:slices s)))
         (= length (count (:slices s)))
         (= l-offset 0))))

(spec/def ::global-offset-consistent? #(global-offsets-check %))


(defn node-plus1 [^AsyncNode node ^ints data-array f ^long iterations]
  (let [{slices :slices, in :ch-in, out :ch-out, res :ch-log, key :key, offset-lut :local-offsets} node]
    (tap> [(str slices) offset-lut])
    (if (= (count slices) 1)
      (a/thread
        (let [id (first slices)
              data-len 1
              local-data (int-array data-len)
              thread-name (format "==> Head [%s]" key)]
          (System/arraycopy data-array id local-data 0 1)
          (timbre/info (format "%s start : (%d)" thread-name (aget local-data 0)))
          ;; iter 0
          (aset local-data 0 (+ (aget local-data 0) 1))
          (timbre/info (format "%s iter %d : (%d)" thread-name 0 (aget local-data 0)))
          (a/>!! out [key local-data])
          (loop [iter (long 1)]
            (let [[k v] (a/<!! in)
                  [offset-v length offset-local] (-> node :local-offsets k)]
              (timbre/info (format "%s, iter %d, got data from %s" thread-name iter k))
              (System/arraycopy v offset-v local-data offset-local length)
              (if (< iter iterations)
                (do (aset local-data 0 ^int (f (aget local-data 0)))
                    (a/>!! out [key (Arrays/copyOf local-data data-len)])
                    (recur (unchecked-inc iter)))
                (do (timbre/info (format " %s iter=%d, done. Sending out local-data" thread-name iter))
                    (a/>!! res [key [local-data (:global-offset node)]])
                    (a/close! res)
                    (a/close! out)))))))
      (a/thread
        (let [id          (first slices)
              data-len    (count slices)
              local-data  (int-array data-len)
              thread-name (format "  ---> Thread [%s]" key)]
          (loop [iter  (long 0)]
            (if (<= iter iterations)
              (let [continue?
                    (boolean
                     (loop [remaining (into #{} (keys offset-lut))]
                       (if (empty? remaining)
                         (do (dotimes [i data-len]
                               (aset local-data i ^int (f (aget local-data i))))
                             (timbre/info (format "%s, iter %d, sending local-data" thread-name iter))
                             (a/>!! out [key (Arrays/copyOf local-data data-len)])
                             true)
                         (if-let [[k v] (a/<!! in)]
                           (if-let [[offset-v length offset-local] (get offset-lut k)]
                             (do (timbre/info (format "%s, iter %d, got : %s" thread-name iter v))
                                 (System/arraycopy v offset-v local-data offset-local length)
                                 (recur (disj remaining k)))
                             (do (timbre/info (format "%s, iter %d, could not find key %s, skip."
                                                      thread-name iter k))
                                 (recur remaining)))
                           (do (timbre/info (format "%s, iter %d: incoming channel is closed. Thread is shutting down."
                                                    thread-name iter))
                               false)))))]
                (if continue?
                  (recur (unchecked-inc iter))
                  (timbre/info (format "%s, iter %d: shutdown." thread-name iter))))
              (do (timbre/info (format "%s, finished." thread-name))))))))))


(defn ^ints test-grid-plus1
  "Every thread will add 1 to each valid entry."
  ([^AsyncGrid grid ^ints src ^ints expected f]
   (test-grid-plus1 grid src expected f 1))
  ([^AsyncGrid grid ^ints src ^ints expected f iterations]
   {:pre [(<= 0 ^long iterations) (= (alength src) (count (grid 0 0))) (= (alength src) (alength expected))]
    :post []}
   (distribute-all grid node-plus1 [src f iterations])
   (let [result (int-array (alength src))]
     (doseq [[k [data [global-offset len local-offset]]] (a/<!! (collect-data grid #(= (count (:slices %)) 1)))]
       (System/arraycopy data local-offset result global-offset len))
     [(Arrays/equals result expected) result])))

