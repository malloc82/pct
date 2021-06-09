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


(defprotocol IAsyncSetup
  (get-overlap              [upstream downstream]    "given an downstream node, find the first continuous overlapping slices")
  (connect-upstream         [this upstream]          "set up a connection with an upstream node")
  (connect-downstream*      [this downstream slices] "** Should only be called from connect-upstream **")
  (disconnect-upstream      [this upstream]          "disconnect with a upstream node")
  (disconnect-downstream*   [this downstream]        "** Should only becalled from disconnect-upstream  **")
  (disconnect-all-upstreams [this]                   "disconnect from all the upstream nodes")
  (upstream-full?           [this]                   "test if upstream connection is saturated")
  (downstream-full?         [this]                   "test if downstream connection is saturated")
  (get-connection-status    [this]                   "get current upstream & downstream connection info as a persistent data"))


(defprotocol IAsyncCommunication
  (asyncTest [this f] "For testing, f args: [in out]"))

(defprotocol IAsyncCompute)

(defprotocol INodeInfo
  (setProperty   [this k v])
  (updateProperty [this k f] [this k f a] [this k f a b])
  (getProperty   [this k])
  (getProperties [this])
  (clearProperties! [this]))

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
                      ^ManyToManyChannel ch-in ^ManyToManyChannel ch-out mux-out ;; data channels
                      ^ManyToManyChannel ch-log ;; data logging, for debugging & testing
                      ^TreeSet unused
                      ^NodeConnection upstream ^NodeConnection downstream
                      ^HashMap local-offsets ^clojure.lang.PersistentVector global-offset
                      Properties]
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
      (if-not (empty? usage)
        (do ;; (.removeAll ^TreeSet (:unused upstream-node) usage)
          ;; (add-node (:downstream upstream-node) key usage)
          (add-node upstream (:key upstream-node) usage)
          (a/tap (:mux-out upstream-node) ch-in)
          (connect-downstream* upstream-node key usage)
          true)
        false)))

  (connect-downstream* [this downstream-key shared-slices]
    (add-node downstream downstream-key shared-slices)
    (.removeAll unused shared-slices))

  (disconnect-upstream [this upstream-node]
    (if-let [usage (get-slices upstream (:key upstream-node))]
      (do (remove-node upstream (:key upstream-node))
          (a/untap (:mux-out upstream-node) ch-in)
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

  ;; IAsyncCommunication
  ;; (distribute [this f]
  ;;   (f this))

  INodeInfo
  (setProperty [this k v]    (swap! Properties assoc k v))
  (updateProperty [this k f] (swap! Properties update k f))
  (getProperty [this k]    (get @Properties k))
  (getProperties [this]    @Properties)
  (clearProperties! [this] (reset! Properties {}))
  )


(defn newAsyncNode [slices]
  {:pre [(spec/valid? ::continuous-slice-ids? slices)]
   :post []}
  (let [ch-out (a/chan 4)
        n (int (count slices))]
    (map->AsyncNode {:ch-in      (a/chan 4)
                     :ch-out     ch-out
                     :mux-out    (a/mult ch-out)
                     :ch-log     (a/chan 4)
                     :key        (keyword (clojure.string/join "-" slices))
                     :slices     (into (sorted-set) slices)
                     :unused     (TreeSet. ^java.util.Collection slices)
                     :upstream   (newNodeConnection n)
                     :downstream (newNodeConnection n)
                     :global-offset [(int (first slices)) n (int 0)] ;; offset-x, length, offset-y
                     :local-offsets (HashMap. n)
                     :properties (atom {})})))


(defprotocol IAsyncGrid
  (get-grid [this])
  (get-table [this])
  (connect-nodes [this])
  (compute-offsets [this])

  (distribute-selected [this cond-fn f] [this cond-fn f args] "apply f to selected nodes, filtered by cond-fn")
  (distribute-all      [this f] [this f args]                 "apply f to every node")
  (collect-data    [this] [this cond-fn]      "Collect one data from each of the selected node, filtered by cond-fn")
  (data-log   [this]  "get a data log channel"))

(def grid-walker (recursive-path [] p (if-path vector? [ALL p] STAY)))

(deftype AsyncGrid [^int slice-count ^int max-block-size
                    ^clojure.lang.PersistentHashMap lut ^clojure.lang.PersistentVector grid]
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
  (get-grid  [this] grid)
  (get-table [this] lut)

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
            (.put local-offsets k  [(- seg-head up-head) (count v) (- seg-head local-head)]))))))

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

  (collect-data [this]
    (collect-data this (fn [_] true)))

  (collect-data [this cond-fn]
    (a/go
      (let [nodes (select [grid-walker cond-fn] grid)
            cs (mapv :ch-log nodes)]
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
      log-ch)))

(defn newAsyncGrid
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

