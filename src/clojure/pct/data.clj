(ns pct.data
  (:use pct.common pct.io)
  (:require pct.util.system pct.async.threads
            [clojure.spec.alpha :as spec]
            [com.rpl.specter :as sp]
            [clojure.java [io :as io]]
            [clojure.string :as s]
            [clojure.core.async :as a :refer  [<!! >!! go go-loop <! >! put! close! alts! chan timeout thread]]
            [taoensso.timbre :as timbre]
            [uncomplicate.fluokitten.core :refer [fmap fmap!]]
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
  (:import [java.util Arrays ArrayList Collection]
           [java.nio ByteOrder ByteBuffer IntBuffer FloatBuffer]
           [java.io FileOutputStream BufferedOutputStream RandomAccessFile File]
           [java.util HashMap HashSet Arrays TreeSet]
           java.lang.AutoCloseable
           [uncomplicate.neanderthal.internal.host.buffer_block IntegerBlockVector RealBlockVector]))

(set! *warn-on-reflection* true)
;; (set! *unchecked-math* true)
(set! *unchecked-math* :warn-on-boxed)
;; (defonce header-length (count (str (Integer/MAX_VALUE))))


(defn ^:private vseq [v ^long len ^long i]
  (lazy-seq
   (if (< i len)
     (cons (nth v i) (vseq v len (unchecked-inc i)))
     '())))


(defn ^:private sseq [s fnext]
  (lazy-seq
   (let [b (fnext s)]
     (if b
       (cons b (sseq s fnext))
       '()))))


(defn ^:private aseq [a ^long len ^long i]
  (lazy-seq
   (if (< i len)
     (cons (aget ^ints a i) (aseq a len (unchecked-inc i)))
     '())))


(defn ^:private nested-array-seq [^ArrayList a1 a1-len a1-idx
                                  ^ArrayList a2 a2-len a2-idx]
  (lazy-seq
   (cons [(.get a2 a2-idx) a1-idx a2-idx]
         (let [next-a2-idx (inc ^long a2-idx)]
           (if (< next-a2-idx ^long a2-len)
             (nested-array-seq a1 a1-len a1-idx
                               a2 a2-len next-a2-idx)
             (loop [next-a1-idx (inc ^long a1-idx)] ;; search for next entry
               (if (< next-a1-idx ^long a1-len)
                 (let [next-a2 ^ArrayList (.get a1 next-a1-idx)
                       next-a2-len (.size next-a2)]
                   (if (> next-a2-len 1)
                     (nested-array-seq a1 a1-len next-a1-idx
                                       next-a2 next-a2-len 1)
                     (recur (inc next-a1-idx))))
                 '()))))))
  #_(lazy-seq
   (if (< ^long a1-idx ^long a1-len)
     (if (< ^long a2-idx ^long a2-len)
       (cons [(.get a2 a2-idx) a1-idx a2-idx]
             (nested-array-seq a1 a1-len a1-idx
                               a2 a2-len (unchecked-inc ^long a2-idx)))
       (let [i (inc ^long a1-idx)]
         (if (< i ^long a1-len)
           (let [next-a ^ArrayList (.get ^ArrayList a1 i)]
             (cons [(.get next-a 0) i 0]
                   (nested-array-seq a1 a1-len i
                                     next-a (.size next-a) 1)))
           '())))
     '())))

#_(defn ^:private b-seq [ ])

(defprotocol ICloseable
  (close* [this])
  (closed?* [this]))

(defprotocol IHistory
  (long-enough?* [this len] "determine if this history path is long enough")
  (tag-sliceIDs* [this offset] [this offset forced] "return a vector of slices IDs this history pass through, given slice offset")
  (get-sliceIDs* [this] [this offset] "return the map/set"))


(defprotocol IPathData
  (toPathData [this] [this x] [this x spec]))

(defprotocol IPathDataInfo
  (slices [this slice-length] [this slice-length start-idx]))

(defprotocol IPathCompute
  (dot* [this x])
  (dot-2* [this x])
  (residue*   [this x])
  (proj_art-1*  [this x lambda])
  (proj_art-2*  [this x lambda])
  (proj_art-3*  [this x lambda])
  (proj_art-4*  [this x lambda])
  (proj_art-5*  [this x lambda])
  (proj_art-6*  [this x lambda])
  (proj_art-7*  [this x lambda])
  (proj_drop* [this x lambda hit-map]))

(defprotocol IPathAccess
  (set-angle!* [this a])
  (get-angle*  [this])
  (set-dot!*   [this d])
  (get-dot*    [this]))

(defprotocol IHistoryInput
  (skip* [this n]   "skip n data")
  (next* [this] [this n] [this n out-ch]
    "read next data, if n is given will read the next n data to acc.
     If acc could be either a container or a channel")
  (rest* [this] [this out-ch] [this out-ch batch] "read the remaining data to out channel")
  ;; (reset* [this]    "reset position")
  (read-PathData [this] [this out-ch] [this out-ch batch] "read data from file as pathdata directly")
  (length* [this] )
  (count-test [this] [this m]))

;; (defprotocol IOutput
;;   (write* [this] [this os] "write this to os")
;;   (close* [this]))

(defprotocol IOutput
  (write-out* [this]))


(defprotocol IAccumulator
  (add* [this obj]))


(defprotocol IHistoryOutput
  (writeHistory* [this history])
  (writeHeader* [this header]))


(defprotocol IHistoryIndex
  ;; (counted?* [this])
  ;; (addHistories*   [this coll slice length])
  ;; (removeHistories* [this i] [this i j])
  (image-size* [this] "return the number of voxels of the full image.")
  (slice-size* [this] "return the size of a slice")
  (mergeIndex*   [this m])
  (set-x0* [this x])
  ;; (index* [this] "return a key map")
  (summary* [this] "Print a summary for current index")
  (trimToSize* [this]))


(defprotocol IVoxelHistogram
  (reset-hit-counts* [this])
  (counted?* [this])
  (count-voxel-hits* [this] [this forced])
  #_(getVoxelCount* [this i] [this i j]))


(deftype PathData [^int id ^ints path ^double chord-len ^float energy
                   ^float entry-xy ^float entry-xz ^float exit-xy ^float exit-xz
                   ^HashMap properties]

  IHistory
  (get-sliceIDs* [this offset]
    (reduce #(conj %1 (quot ^long %2 ^long offset)) (sorted-set) path))

  IPathDataInfo
  (slices [this slice-offset] (slices this slice-offset 0))
  (slices [this slice-offset start-idx]
    (let [len (alength path)
          s (new TreeSet)]
      (loop [i (long 0)]
        (if (< i len)
          (do (.add s (+ ^int start-idx (quot ^int (aget path i) ^int slice-offset)))
              (recur (unchecked-inc i)))
          s))))

  java.lang.Comparable
  (compareTo [this o]
    (cond
      ;; (identical? this o) (int 0)
      (> entry-xy ^float (.entry-xy ^PathData o)) (int  1)
      (< entry-xy ^float (.entry-xy ^PathData o)) (int -1)
      (= entry-xy ^float (.entry-xy ^PathData o)) (int  0)))

  IPathCompute
  (dot* [this x]
    (let [n ^int (alength path)]
      (loop [i (long 0)
             sum (double 0.0)]
        (if (< i n)
          (recur (unchecked-inc i) (+ sum ^double (.entry ^RealBlockVector x ^int (aget path i))))
          sum))))

  (dot-2* [this x]
    (let [n ^int (alength path)]
      (loop [i (long 0)
             sum (double 0.0)]
        (if (< i n)
          (recur (unchecked-inc i) (+ sum ^double (aget ^doubles x ^int (aget path i))))
          sum))))

  (residue* [this x]
    (- ^double (dot* this x) (/ (double energy) chord-len)))

  (proj_art-1* [this x lambda]
    (let [n ^int (alength path)
          a ^double (* ^double lambda (/ (- energy (* ^double (dot* this x) chord-len))
                                         (* n chord-len)))]
      (loop [k (int 0)]
        (if (< k n)
          (let [i ^int (aget path k)
                xi (double (x i))]
            (if (not= xi 0.0)
              (x i (+ xi a)))
            (recur (unchecked-inc-int k)))
          x))))

  (proj_art-2* [this x lambda]
    (let [n ^int (alength path)
          a ^double (* ^double lambda (/ (- (/ energy chord-len) ^double (dot* this x))
                                         n ))]
      (loop [k (int 0)]
        (if (< k n)
          (let [i ^int (aget path k)
                xi (double (x i))]
            (if (not= xi 0.0)
              (x i (+ xi a)))
            (recur (unchecked-inc-int k)))
          x))))

  ;; second fastest
  (proj_art-3* [this x lambda]
    (let [n ^int (alength path)
          a ^double (* ^double lambda (/ (- (/ energy chord-len) ^double (dot* this x))
                                         n ))]
      (loop [k (long 0)]
        (if (< k n)
          (let [i ^int (aget path k)
                xi ^double (.entry ^RealBlockVector x i)
                next-k (unchecked-inc k)]
            (if (= xi 0.0)
              (recur next-k)
              (do (.set ^RealBlockVector x i (+ xi a))
                  (recur next-k))))
          x))))

  ;; Using double array seems to be the fastest so far
  (proj_art-4* [this x lambda]
    (let [n ^int (alength path)
          a ^double (* ^double lambda (/ (- (/ energy chord-len) ^double (dot-2* this x))
                                         n ))]
      (loop [k (long 0)]
        (if (< k n)
          (let [i ^int (aget path k)
                xi ^double (aget ^doubles x i)
                v  (unchecked-add xi a)
                next-k (unchecked-inc k)]
            (if (= xi 0.0)
              (recur next-k)
              (do (aset ^doubles x i v)
                  (recur next-k))))
          x))))

  (proj_art-5* [this x lambda]
    (let [n ^int (alength path)
          a (loop [i (long 0)
                   sum (double 0.0)]
              (if (< i n)
                (recur (+ i 1) (+ sum ^double (aget ^doubles x ^int (aget path i))))
                (* ^double lambda
                   ^double (/ (- (/ energy chord-len) sum)
                              n))))]
      (loop [k (long 0)]
        (if (< k n)
          (let [i  ^int    (aget path k)
                xi ^double (aget ^doubles x i)
                next-k (unchecked-inc k)]
            (if (= xi 0.0)
              (aset ^doubles x i (+ xi ^double a)))
            (recur next-k))
          x))))

  (proj_art-6* [this x lambda]
    (let [n ^int (alength path)
          a (* ^double lambda
               ^double (/ (- (/ energy chord-len)
                             ^double (reduce + 0.0 (fmap #(aget ^doubles x %) path)))
                          n))]
      (loop [k (long 0)]
        (if (< k n)
          (let [i  ^int    (aget path k)
                xi ^double (aget ^doubles x i)
                v  (+ xi a)
                next-k (+ k 1)]
            (if (= xi 0.0)
              (recur next-k)
              (do (aset ^doubles x i v)
                  (recur next-k))))
          x))))

  #_(proj_art-3* [this x lambda]
      (let [n ^int (alength path)
            a ^double (* ^double lambda (/ (- (/ energy chord-len) ^double (dot* this x))
                                           n ))
            f (fn ^double [^double v] (if (= v 0.0) 0.0 (+ v a)))]
        (loop [k (int 0)]
          (if (< k n)
            (do (alter! ^RealBlockVector x ^int (aget path k) f)
                (recur (unchecked-inc-int k)))
            x))))

  #_(proj_art-4* [this x lambda]
    (let [n ^int (alength path)
          a ^double (* ^double lambda (/ (- (/ energy chord-len) ^double (dot* this x))
                                         n ))]
      (loop [k (int 0)]
        (if (< k n)
          (let [i ^int (aget path k)]
            (alter! ^RealBlockVector x i (fn ^double [^double v] (if (= v 0.0) 0.0 (+ v a))))
            (recur (unchecked-inc-int k)))
          x))))

  (proj_drop* [this x lambda hit-map]
    (let [n ^int (alength path)
          a ^double (* ^double lambda (/ (- energy ^double (dot* this x))
                                         (* n chord-len)))]
      (loop [k (int 0)]
        (if (< k n)
          (let [i ^int (aget path k)
                xi (double (x i))]
            (if (not= xi 0.0)
              (x i (+ xi (/ a ^double (hit-map i)))))
            (recur (unchecked-inc-int k)))
          x))))

  clojure.lang.IFn
  (invoke [this i] (aget path i))
  (invoke [this] energy)

  clojure.lang.Counted
  (count [_] (alength path))

  clojure.lang.IPersistentVector
  (seq [_] (aseq path (alength path) 0))
  (nth [this i] (aget path i))
  (nth [this i not-found]
    (if (< i (alength path))
      (aget path (int i))
      not-found))

  java.util.Collection
  (equals [this o]
    (cond
      (nil? o) false
      (identical? this o) true
      (instance? PathData o) (let [n ^int (.size ^PathData o)]
                               (and (= energy (.energy ^PathData o))
                                    (= chord-len (.chord-len ^PathData o))
                                    (= (alength path) n)
                                    (loop [i n]
                                      (if (>= i 0)
                                        (if (= (aget path i) (nth ^PathData o i))
                                          (recur (unchecked-dec-int i))
                                          false)
                                        true))))
      :default false))
  (size [_] (alength path))
  (isEmpty [_] (= (alength path) 0))
  (iterator [_] (clojure.lang.SeqIterator. (aseq path (alength path) 0))))



(defn PathData->fromStream
  "Read the path and b data from two seperate files provided by Paniz, each one contains everything.
   pis : BufferedInputStream for path file
   bis : BufferedInputstream for b file

   Return a PathData"

  ([^long id ^java.io.BufferedInputStream pis ^java.io.BufferedInputStream bis]
   (when-let [[^int length ^bytes len-buf] (read-int pis true)]
     (let [path-buf ^bytes (byte-array (* 4 length))
           path-arr ^ints  (int-array length)]
       (when-not (= (.read pis path-buf) -1)
         (-> (ByteBuffer/wrap ^bytes path-buf)
             (.order ByteOrder/LITTLE_ENDIAN)
             .asIntBuffer
             (.get path-arr))
         (let [[^double chord-len   _] (read-double pis true)
               [^FloatBuffer angles _] (read-floats pis 4 true)
               [^float energy       _] (read-float  bis true)]
           (->PathData id path-arr chord-len energy
                       (.get angles 0) (.get angles 1) (.get angles 2) (.get angles 3)
                       (new HashMap))))))))

(deftype HistoryInputStream [^java.io.BufferedInputStream pis ^java.io.BufferedInputStream bis ^long length
                             ^{:unsynchronized-mutable true :tag long} index
                             ^{:unsynchronized-mutable true :tag boolean} open?]
  IHistoryInput
  ;; (skip* [this k]
  ;;   (let [last-idx (+ index ^long k)]
  ;;     (if bis
  ;;       (loop [i index]
  ;;         (if (< i last-idx)
  ;;           (when-let [b (HistoryBuffer->fromStream i pis bis)]
  ;;             (recur (unchecked-inc i)))
  ;;           (set! index i)))
  ;;       (loop [i index]
  ;;         (if (< i last-idx)
  ;;           (when-let [b (HistoryBuffer->fromStream i pis bis)]
  ;;             (recur (unchecked-inc i)))
  ;;           (set! index i))))
  ;;     this))

  ;; (next* [this]
  ;;   (when open?
  ;;     (let [s (HistoryBuffer->fromStream index pis bis)]
  ;;       (set! index (inc index))
  ;;       s)))

  ;; (next* [this n]
  ;;   (let [acc ^ArrayList (ArrayList. (int n))
  ;;         last-idx (+ index ^long n)]
  ;;     (loop [i index]
  ;;       (if (< i last-idx)
  ;;         (when-let [b (HistoryBuffer->fromStream i pis bis)]
  ;;           (.add acc b)
  ;;           (recur (inc i)))
  ;;         (set! index i)))
  ;;     acc))

  ;; (next* [this n out-ch]
  ;;   (let [last-idx (+ index ^long n)]
  ;;     (loop [i index]
  ;;       (if (< i last-idx)
  ;;         (when-let [b (HistoryBuffer->fromStream i pis bis)]
  ;;           (>!! out-ch b)
  ;;           (recur (inc i)))
  ;;         (set! index i))))
  ;;   (a/close! out-ch))

  ;; (rest* [this out-ch] ;; read
  ;;   (loop []
  ;;     (when-let [b (HistoryBuffer->fromStream index pis bis)]
  ;;       (set! index (inc index))
  ;;       (>!! out-ch b)
  ;;       (recur)))
  ;;   (a/close! out-ch))

  ;; (rest* [this out-ch batch-size]
  ;;   ;; read in data as Historybuffer
  ;;   (let [batch-size ^long batch-size]
  ;;     (loop [acc ^objects (object-array batch-size)
  ;;            i   ^long    (long 0)
  ;;            c index]
  ;;       (if-let [b (HistoryBuffer->fromStream c pis bis)]
  ;;         (do (if (< i batch-size)
  ;;               (do (aset acc i b)
  ;;                   (recur acc (unchecked-inc i) (unchecked-inc c)))
  ;;               (let [new-acc ^objects (object-array batch-size)]
  ;;                 (>!! out-ch [i acc])
  ;;                 (aset new-acc 0 b)
  ;;                 (recur new-acc (long 1) (unchecked-inc c)))))
  ;;         (do (>!! out-ch [i acc])
  ;;             (set! index c))))
  ;;     (a/close! out-ch)))

  (read-PathData [this]
    (if-let [s (PathData->fromStream index pis bis)]
      (do (set! index (inc index))
          s)))

  (read-PathData [this out-ch]
    (loop []
      (when-let [b (PathData->fromStream index pis bis)]
        (set! index (inc index))
        (>!! out-ch b)
        (recur)))
    (a/close! out-ch))

  (read-PathData [this out-ch batch-size]
    ;; read in data as PathData
    (let [batch-size ^long batch-size]
      (loop [acc ^objects (object-array batch-size)
             len ^long    (long 0)
             id  ^long    (long 0)]
        (if-let [b (PathData->fromStream id pis bis)]
          (if (< len batch-size)
            (do (aset acc len b)
                (recur acc (unchecked-inc len) (unchecked-inc id)))
            (let [new-acc ^objects (object-array batch-size)]
              (>!! out-ch [len acc])
              (aset new-acc 0 b)
              (recur new-acc (long 1) (unchecked-inc id))))
          (do (>!! out-ch [len acc]))))
      (.close this)
      (a/close! out-ch)))

  (count-test [this]
    (loop [i ^long (long 0)]
      (if-let [b (PathData->fromStream i pis bis)]
        (recur (unchecked-inc i))
        i)))

  (count-test [this m]
    (let [n (.size ^HashMap m)
          res ^HashMap (HashMap.)]
     (loop [i          ^long (long 0)
            test-count ^long (long 0)]
       (if (< test-count n)
         (if-let [b ^PathData (PathData->fromStream i pis bis)]
           (do (if-let [arr ^ints (.get ^HashMap m i)]
                 (do (if (Arrays/equals arr ^ints (.path b))
                       (.put res i true)
                       (.put res i {:path (.path b) :sample arr}))
                     (recur (unchecked-inc i) (unchecked-inc test-count)))
                 (recur (unchecked-inc i) test-count))))
         res))))

  (length* [_] length)

  ICloseable
  (close* [this]
    (.close this))

  java.lang.AutoCloseable
  (close [this]
    ;; (print "closing stream ... ")
    (timbre/info "closing HistoryInputStream")
    (.close pis)
    (when bis (.close bis))
    (set! open? (boolean false))
    ;; (println "done.")
    )

  clojure.lang.Seqable
  (seq [this]
    (sseq this next*))
  clojure.lang.Counted
  (count [_] length))

(defn newHistoryInputStream
  ([path-file]
   (let [pis ^java.io.BufferedInputStream (io/input-stream path-file)
         p-count ^int (read-header pis)]
     (HistoryInputStream. pis nil p-count 0 true)))
  ([path-file b-file]
   (let [pis ^java.io.BufferedInputStream (io/input-stream path-file)
         bis ^java.io.BufferedInputStream (io/input-stream b-file)
         p-count ^int (read-header pis)
         b-count ^int (read-header bis)]
     (assert (= p-count b-count))
     (HistoryInputStream. pis bis p-count 0 true))))


;; (deftype HistoryOutputStream [^String name ^RandomAccessFile raf ^BufferedOutputStream os
;;                            ^{:unsynchronized-mutable true :tag int} n]
;;   IHistoryOutput
;;   (writeHistory* [this history]
;;     (let [len-buf  ^bytes (.len-buf  ^HistoryBuffer history)
;;           path-buf ^bytes (.path-buf ^HistoryBuffer history)
;;           e-buf    ^bytes (.e-buf    ^HistoryBuffer history)]
;;       (if (and len-buf path-buf e-buf)
;;         (do
;;           (.write os len-buf  0 (alength len-buf))
;;           (.write os path-buf 0 (alength path-buf))
;;           (.write os e-buf    0 (alength e-buf))
;;           (set! n (unchecked-inc-int n))
;;           true)
;;         false)))
;;   (writeHeader* [this header]
;;     (.flush os)
;;     (let [n ^long (.getFilePointer raf)]
;;       (.seek raf 0)
;;       (.writeBytes raf (format (format "%%0%dd\n" header-length) header))
;;       (when (> n 0)
;;         (.seek raf n))))

;;   ICloseable
;;   (close* [this]
;;     (.close this))

;;   java.lang.AutoCloseable
;;   (close [this]
;;     ;; (print "closing output stream ...")
;;     (writeHeader* this n)
;;     (.close os)
;;     (.close raf)
;;     ;; (println "done.")
;;     ))

;; (defn newHistoryOutputStream
;;   ([fname]
;;    (newHistoryOutputStream fname 8192))
;;   ([fname size]
;;    (let [f ^File (File. ^String fname)
;;          raf ^RandomAccessFile (RandomAccessFile. f "rw")
;;          bos ^BufferedOutputStream (BufferedOutputStream. (FileOutputStream. (.getFD raf)) (int size))]
;;      (.writeBytes raf (format (format "%%0%dd\n" header-length) 0))
;;      (HistoryOutputStream. fname raf bos (int 0)))))

(defn ^:private ensureSize [^ArrayList arr ^long n f]
  (when (<= (count arr) (int n))
    (loop [i (- n (count arr))]
      (when (>= i 0)
        (.add arr (f))
        (recur (dec i))))))


;; ====================================================================================================
;; HistoryIndex
;; ====================================================================================================


(defn voxel-count-batch [histories rows cols slices]
  #_(let [n (count histories)]
    #_(timbre/info (format "Start counting voxels for %d histories" n))
    (let [res (reduce (fn [^RealBlockVector acc ^PathData h]
                        (reduce (fn [^RealBlockVector acc idx] (acc idx (inc ^double (acc idx))))
                                acc
                                h))
                      (dv (* ^long rows ^long cols ^long slices))
                      histories)]
      #_(timbre/info (format "Finished counting voxels for %d histories" n))
      res))
  (let [acc (dv (* ^long rows ^long cols ^long slices))
        it  (clojure.lang.RT/iter histories)]
    (loop []
      (when (.hasNext it)
        (let [path ^PathData (.next it)
              arr ^ints (.path ^PathData path)
              len (alength arr)]
          (loop [i (long 0)]
            (if (< i len)
              (let [idx (aget arr i)
                    c ^double (acc idx)]
                (acc idx (+ ^double c 1))
                (recur (unchecked-inc i))))))
        (recur)))
    acc))

(def index-walker (sp/recursive-path [] p (sp/if-path #(instance? java.util.ArrayList %) [sp/ALL p] sp/STAY)))

(deftype HistoryIndex [^ArrayList index
                       ^int rows ^int cols ^int slices
                       ^{:unsynchronized-mutable true :tag int} total
                       ^{:unsynchronized-mutable true :tag boolean} _counted?
                       ^boolean global?
                       ^RealBlockVector x0
                       ,]

  Object
  (equals [this other]
    (identical? this other))

  clojure.lang.Counted
  (count [_] total)

  clojure.lang.Seqable
  (seq [_]
    (let [len (.size index)]
      (if (> len 0)
        (loop [i (long 0)] ;; find the first non-empty entry
          (if (< i len)
            (let [a ^ArrayList (.get index i)
                  n (.size a)]
              (if (> n 1) ;;
                (nested-array-seq index (.size index) i
                                  a n 1)
                (recur (unchecked-inc i))))
            '()))
        '())))

  clojure.lang.IFn
  (invoke [this i j k]
    (case (int k)
      0 ((.invoke this i j) 0)
      1 ((.invoke this i j) 1)
      nil)
    #_(case k
        0 (when (< ^int i (.size index))
            (let [slice ^ArrayList (.get index ^int i)]
              (when (< ^int j (.size slice))
                ((.get slice ^int j) 0))))
        1 (when (< ^int i (.size index))
            (let [slice ^ArrayList (.get index ^int i)]
              (when (< ^int j (.size slice))
                ((.get slice ^int j) 1))))
        nil))
  (invoke [this i j]
    (when (< ^int i (.size index))
      (let [slice ^ArrayList (.get index ^int i)]
        (when (< ^int j (.size slice))
          (.get slice ^int j)))))
  (invoke [this i]
    (when (< ^int i  (.size index))
      (.get index ^int i)))
  (invoke [this]
    (let [n ^int (.size index)]
      (loop [m (sorted-map)
             i (int 0)]
        (if (< i n)
          (let [slice ^ArrayList (.get index i)
                s ^int (.size slice)
                m2 (loop [m2 (sorted-map)
                          j (int 0)]
                     (if (< j s)
                       (recur (assoc m2 j (count (.get slice j)))
                              (unchecked-inc-int j))
                       m2))]
            (recur (assoc m i m2)
                   (unchecked-inc-int i)))
          m))))
  (applyTo [this s]
    ;; (println "Calling applyTo: " (count s))
    (case (count s)
      0 (.invoke this)
      1 (let [[a] s]
          (.invoke this a))
      2 (let [[a b] s]
          (.invoke this a b))
      3 (let [[a b c] s]
          (.invoke this a b c))
      (throw (UnsupportedOperationException.))))

  IHistoryIndex
  ;; (counted? [this] counted?)

  ;; (addHistories* [this hist slice length]
  ;;   (ensureSize index slice #(ArrayList.))
  ;;   (let [s-entry ^ArrayList (.get index slice)]
  ;;     (ensureSize s-entry length #(ArrayList.))
  ;;     (let [e ^ArrayList (.get s-entry length)]
  ;;       (.addAll e hist)
  ;;       (set! total (unchecked-add-int total (count hist))))))

  ;; (removeHistories* [this i]
  ;;   (when (< (int i) (.size index))
  ;;     (let [slice ^ArrayList (.get index i)
  ;;           n ^int (reduce unchecked-add-int (int 0) (mapv #(.size ^ArrayList %) slice))]
  ;;       (set! total (unchecked-subtract-int total n))
  ;;       (.clear slice)
  ;;       this)))


  ;; (removeHistories* [this i j]
  ;;   (when (< (int i) (.size index))
  ;;     (let [slice ^ArrayList (.get index i)]
  ;;       (when (< (int j) (.size slice))
  ;;         (let [entry ^ArrayList (.get slice j)]
  ;;           (set! total (unchecked-subtract-int total (.size entry)))
  ;;           (.clear entry)
  ;;           this)))))

  (image-size* [this]
    (* rows cols slices))

  (slice-size* [this]
    (* rows cols))

  (mergeIndex* [this m]
    (let [idx-it (clojure.lang.RT/iter m)]
      (loop [sub-total (long 0)]
        (if (.hasNext idx-it)
          (let [[^int idx ^HashMap length-map] (.next idx-it)
                len-it (clojure.lang.RT/iter length-map)
                sliceIndex-idx ^ArrayList (.get index idx)]
            (recur (long (loop [acc sub-total]
                           (if (.hasNext len-it)
                             (let [[^int len ^ArrayList data] (.next len-it)
                                   [^ArrayList sliceIndex-idx-len _] (.get sliceIndex-idx len)]
                               (.addAll sliceIndex-idx-len data)
                               #_(set! total (unchecked-add-int total (.size data)))
                               (recur (unchecked-add acc (.size data))))
                             acc)))))
          (do (set! total (unchecked-add-int total sub-total))))))
    #_(loop [m1 m]
      (when-let [[[^int s ^ArrayList m2] & rst-m1] m1]
        (let [entry ^ArrayList (.get index s)]
          (loop [m2 m2]
            (when-let [[[^int l ^ArrayList hist] & rst-m2] m2]
              (ensureSize entry l #(ArrayList.))
              (.addAll ^ArrayList (.get entry l) hist)
              (set! total (unchecked-add-int total (count hist)))
              (recur rst-m2))))
        (recur rst-m1))))

  (set-x0* [this x]
    (copy! ^RealBlockVector x x0))

  (summary* [this]
    (let [N (count index)]
      (loop [i 0]
        (when (< i N)
          (print (format "%2d : " i))
          (let [slice ^ArrayList (.get index i)
                K (count slice)]
            (loop [j 0]
              (when (< j K)
                (print (format " [%d] " (count (.get slice j))))
                (recur (unchecked-inc j)))))
          (println "")
          (recur (unchecked-inc i))))
      (println "Total : " total)))

  (trimToSize* [this]
    (.trimToSize index)
    (let [n_s ^int (.size index)]
      (loop [i (int 0)]
        (if (< i n_s)
          (let [slice ^ArrayList (.get index i)
                n_l ^int (.size slice)]
            (loop [k (int 0)]
              (when (< k n_l)
                (let [e ^ArrayList (.get slice k)]
                  (.trimToSize e))
                (recur (unchecked-inc-int k))))
            (recur (unchecked-inc-int i)))
          this))))

  IVoxelHistogram
  (reset-hit-counts* [this]
    (dotimes [i (.size index)]
      (let [slice-idx ^ArrayList (.get index i)]
        (dotimes [len (.size slice-idx)]
          (let [[_ ^RealBlockVector hits] (.get slice-idx len)]
            (alter!  hits (fn ^double [^double _] 0.0)))))))

  (counted?* [this] )

  (count-voxel-hits* [this opts]
    (let [{jobs       :jobs
           batch-size :batch-size,
           :or {jobs (- ^int pct.util.system/PhysicalCores 4) batch-size 250000}} opts]
      (timbre/info (format "===> Counting voxel hits: [jobs %d, batch-size %d]" jobs batch-size))
      (let [data-ch (a/chan jobs)
            res-ch (pct.async.threads/asyncWorkers
                    jobs
                    ;; workers
                    (fn [[[^ArrayList data ^long i ^long data-len] ^RealBlockVector hits ^int start-idx ^int len]]
                      (try
                        (let [hits ^RealBlockVector (zero hits)
                              data-len (min (+ i ^long batch-size) data-len)]
                          (loop [i (long i)]
                            (if (< i data-len)
                              (let [path ^PathData (.get data i)
                                    arr  ^ints     (.path ^PathData path)
                                    alen ^int      (alength arr)]
                                (loop [j (long 0)]
                                  (if (< j alen)
                                    (let [idx ^int (aget arr j)
                                          c ^double (hits idx)]
                                      (hits idx (+ ^double c 1.0))
                                      (recur (unchecked-inc j)))))
                                (recur (unchecked-inc i)))
                              [hits start-idx len])))
                        (catch Exception ex
                          (timbre/error ex (format "[dim = %d, i = %d, start-idx = %d, len = %d]" (dim hits) i start-idx len))
                          (throw ex))))
                    ;; accumulator
                    (fn
                      ([] this)
                      ([acc] acc)
                      ([acc batch]
                       #_(timbre/info (format "Updating branch [%d, %d]" start-idx len))
                       (if-let [[^RealBlockVector v-hits ^int start-idx ^int len] batch]
                         (if-let [[_ h] (.invoke this start-idx len)]
                           (do (axpy! v-hits h)
                               (release v-hits))
                           (timbre/error "Got nil, [%d, %d]" start-idx len))
                         (timbre/error "Got nil"))
                       acc))
                    data-ch)]
        (let []
          ;; dispatch
          (try
            (timbre/info (format "new method of voxel counting, job = %d, batch-size = %d" jobs batch-size))
            (loop [s (seq this)] ;; better loop
              (if-let [[[^ArrayList data hits] ^long start-idx ^long len] (first s)]
                (let [data-len (long (.size data))]
                  (alter! hits (fn ^double [^double _] 0.0)) ;; reset hit map
                  (loop [i (long 0)]
                    (when (<  i  data-len)
                      (a/>!! data-ch [[data i data-len] hits start-idx len])
                      (recur (unchecked-add i ^long batch-size))))
                  #_(loop [parts (partition-all batch-size data)]
                    (when parts
                      (a/>!! data-ch [(first parts) hits start-idx len])
                      (recur (next parts))))
                  (recur (next s)))
                (a/close! data-ch)))
            (catch Exception ex
              (a/close! data-ch)
              (timbre/error ex "[count-voxel-hits*] Something went wrong during dispatch." (.getName (Thread/currentThread)))))
          (a/<!! res-ch)
          (set! _counted? (boolean true))
          this))))


  ;; (getVoxelCount* [this i]
  ;;   (when (< ^long i (.size index))
  ;;     (when-let [s ^ArrayList (.get index i)]
  ;;       (let [n (.size s)
  ;;             [_ h] (.invoke this i (dec n))
  ;;             acc ^RealBlockVector (copy h)]
  ;;         (loop [k (- n 2)]
  ;;           (if (> k 0)
  ;;             (let [[_ h] (.get s k)]
  ;;               (axpy! h acc 0 (dim h) 0)
  ;;               (recur (unchecked-dec-int k)))
  ;;             acc))))))

  ;; (getVoxelCount* [this i j]
  ;;   (if-let [[_ h] (.invoke this i j)]
  ;;     (copy h)))
  )

(defn total-voxel-hits [^HistoryIndex index method]
  (case method
    :pathdata (reduce + (sp/transform sp/ALL
                                      (fn [[[data _] _ _]]
                                        (reduce (fn ^long [^long acc p] (+ acc (count p))) 0 data))
                                      (seq index)))
    :hitmap (long (reduce + (sp/transform sp/ALL
                                          (fn [[[_ hits] _ _]]
                                            (sum hits))
                                          (seq index))))
    -1))

(defn count-voxel-hits [^HistoryIndex index opts]
  (count-voxel-hits* index opts))


(defn verify-path-length [^ArrayList arr len]
  (let [n (.size arr)]
    (loop [i (long 0)]
      (let [sample ^PathData (.get arr (rand-int n))
            [begin end] (min-max-ints (.path sample))]))))

(defn verify-index-structure [^HistoryIndex index global?]
  (let [full-length  (image-size* index)
        slice-length (slice-size* index)]
    (loop [s (seq index)]
      (if-let [[data ^long idx ^long len] (first s)]
        (do #_(tap> [[(if arr (.size arr)) (if v (dim v))] idx len])
            (if (= len 0)
              (if (nil? data)
                (recur (next s))
                (throw (Exception. (format "HisotryIndex sturcture fault at [%d %d], expected nil for but got something else: %s." idx len (class data)))))
              (let [[^ArrayList arr ^RealBlockVector v] data
                    n (if global? full-length (* ^long slice-length  len))]
                (if (and (instance? ArrayList arr) (= (dim v) n))
                  (recur (next s))
                  (throw (Exception. (format "HisotryIndex sturcture fault at [%d %d], expected hit map size to be %d, got %d."
                                             idx len n (dim v))))))))
        true))))

(defn newHistoryIndex
  (^HistoryIndex [rows cols slices & {:keys [global? x0]}]
   {:pre [(int? rows) (int? cols) (int? slices)
          (or (nil? x0)
              (and (instance? RealBlockVector x0)
                   (= (dim x0) (* ^int rows ^int cols ^int slices))))]
    :post [(verify-index-structure % global?)]}
   (let [rows   ^long rows
         cols   ^long cols
         slices ^long slices
         index ^ArrayList (ArrayList. slices)]

     (if global?
       (dotimes [i slices]
         (.add index (ArrayList. ^Collection (mapv (fn [^long n]
                                                     (if (= n 0)
                                                       nil
                                                       [(ArrayList.) (dv (* rows cols slices))]))
                                                   (range (inc (- slices i)))))))
       (dotimes [i slices]
         (.add index (ArrayList. ^Collection (mapv (fn [^long n]
                                                     (if (= n 0)
                                                       nil
                                                       (do #_(tap> [n (* rows cols n)])
                                                           [(ArrayList.) (dv (* rows cols n))])))
                                                   (range (inc (- slices i))))))))
     (->HistoryIndex index rows cols slices (int 0) false
                     (if global? true false)
                     (if x0 (copy x0) (dv (* rows cols slices)))))))

