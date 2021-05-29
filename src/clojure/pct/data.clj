(ns pct.data
  (:use pct.common pct.io)
  (:require pct.util.system pct.async.threads
            [com.rpl.specter :as sp]
            [clojure.java [io :as io]]
            [clojure.string :as s]
            [clojure.core.async :as a :refer  [<!! >!! go go-loop <! >! put! close! alts! chan timeout thread]]
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
  (:import [java.util Arrays ArrayList Collection]
           [java.nio ByteOrder ByteBuffer IntBuffer FloatBuffer]
           [java.io FileOutputStream BufferedOutputStream RandomAccessFile File]
           [java.util HashMap HashSet Arrays]
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


(defn ^:private hist-seq [^ArrayList arr1 n ^ArrayList arr2 m i j]
  (lazy-seq
   (if (< ^long i ^long n)
     (if (< ^long j ^long m)
       (cons [(.get arr2 j) i j] (hist-seq arr1 n arr2 m i (unchecked-inc ^long j)))
       (let [i (inc ^long i)]
         (if (< i ^long n)
           (let [arr3 ^ArrayList (.get ^ArrayList arr1 i)]
             (cons [(.get arr3 0) i 0] (hist-seq arr1 n arr3 (.size arr3) i 1)))
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


(defprotocol IPathCompute
  (dot* [this x])
  (residue*   [this x])
  (proj_art*  [this x lambda])
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
  (addHistories*   [this coll slice length])
  (removeHistories* [this i] [this i j])
  (mergeIndex*   [this m])
  (index* [this] "return a key map")
  (summary* [this] "Print a summary for current index")
  (trimToSize* [this])
  (branch-seq* [this]))


(defprotocol IVoxelHistogram
  (reset-hit-counts* [this])
  (counted?* [this])
  (count-voxel-hits* [this] [this forced])
  (getVoxelCount* [this i] [this i j]))

#_(deftype Index [^ArrayList index]
  clojure.lang.IFn
  (invoke [this i j]
    (-> index (.get i) (.get j)))
  (invoke [this i j f]
    (f (-> index (.get i) (.get j)))))


(deftype PathData [^int id ^ints path ^double chord-len
                   ^float entry-xy ^float entry-xz ^float exit-xy ^float exit-xz ^float path-angle
                   ^float energy
                   ^double residue
                   ^{:unsynchronized-mutable true :tag HashSet} hashset]
  IHistory
  (get-sliceIDs* [this offset]
    (reduce #(conj %1 (quot ^long %2 ^long offset)) (sorted-set) path))

  java.lang.Comparable
  (compareTo [this o]
    (cond
      (identical? this o) true
      (not (instance? PathData o)) false
      :else (compare residue (.residue ^PathData o))))

  IPathCompute
  (dot* [this x]
    (let [n ^int (alength path)]
      (loop [i (int 0)
             sum (double 0.0)]
        (if (< i n)
          (recur (unchecked-inc-int i) (unchecked-add sum ^double (x (aget path i))))
          (unchecked-multiply sum chord-len)))))

  (residue* [this x]
    (- ^double (dot* this x) (double energy)))

  (proj_art* [this x lambda]
    (let [n ^int (alength path)
          a ^double (* ^double lambda (/ (- energy ^double (dot* this x))
                                         (* n chord-len)))]
      (loop [k (int 0)]
        (if (< k n)
          (let [i ^int (aget path k)
                xi (double (x i))]
            (if (not= xi 0.0)
              (x i (+ xi a)))
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


(deftype HistoryBuffer [^int id
                        ^int path-length ^IntBuffer path
                        ^double chord-len ^float entry-xy ^float entry-xz ^float exit-xy ^float exit-xz
                        ^float energy
                        ^bytes len-buf ^bytes path-buf ^bytes chord-len-buf ^bytes angle-buf ^bytes e-buf
                        ^HashMap slices]
  Object
  (equals [this o]
    (and (= path-length (.path-length ^HistoryBuffer o))
         (.equals path ^IntBuffer (.path ^HistoryBuffer o))
         (= energy (.energy ^HistoryBuffer o))))

  IHistory
  (long-enough?* [this len] (>= path-length ^long len))
  (tag-sliceIDs* [this offset] (tag-sliceIDs* this offset false))
  (tag-sliceIDs* [this offset forced]
    (when forced (.clear slices))
    (if (.isEmpty slices)
      (loop [i ^int (int 0)]
        (if (< i path-length)
          (let [idx ^int (.get path i)
                sid ^int (int (quot idx ^int offset))]
            (if-let [c ^int (.get slices sid)]
              (.replace slices sid (unchecked-inc-int c))
              (.put slices sid (int 1)))
            (recur (unchecked-inc-int i)))
          (do
            (doseq [[k v] slices]
              (.replace slices (int k) (/ (double v) path-length)))
            this)))
      this))

  clojure.lang.Counted
  (count [_] path-length)

  clojure.lang.IFn
  (invoke [this i] (.get path (int i)))

  clojure.lang.Sequential
  clojure.lang.IPersistentVector
  (seq [this]
    (vseq this path-length 0))
  (nth [this i] (.get path (int i)))
  (nth [this i not-found]
    (if (< i path-length)
      (.get path (int i))
      not-found))

  IPathData
  ;; (toPathData [this]
  ;;   (let [pd ^PathData (->PathData (int-array path-length) chord-len energy)
  ;;         a ^ints (.path pd)]
  ;;     (loop [i ^int (int 0)]
  ;;       (when (< i path-lengt
  ;;         (aset a i (.get path i))
  ;;         (recur (unchecked-inc-int i))))
  ;;     pd))
  (toPathData [this]
    (let [a (int-array path-length)]
      (loop [i ^int (int 0)]
        (if (< i path-length)
          (do
            (aset a i (.get path i))
            (recur (inc i)))
          (->PathData id a chord-len
                      entry-xy entry-xz exit-xy exit-xz 0.0
                      energy -1 nil)))))

  (toPathData [this x]
    (let-release [a (int-array path-length)]
      (loop [i   ^int (int 0)
             len ^int (int 0)]
        (if (< i path-length)
          (let [voxl ^int (.get path i)]
            (if (= (x voxl) 0.0)
              (recur (inc i) len)
              (do
                (aset a len voxl)
                (recur (inc i) (inc len)))))
          ;; (timbre/info "Path: " len (vec a))
          (->PathData id (int-array len a) chord-len
                      entry-xy entry-xz exit-xy exit-xz -360.0 ;; -360.0 means that it hasn't been calculated
                      energy (residue* this x) nil)))))
  (toPathData [this x spec]
    (let-release [a (int-array path-length)]
      (loop [i   ^int (int 0)
             len ^int (int 0)]
        (if (< i path-length)
          (let [voxl ^int (.get path i)]
            (if (= (x voxl) 0.0)
              (recur (inc i) len)
              (do
                (aset a len voxl)
                (recur (inc i) (inc len)))))
          ;; (timbre/info "Path: " len (vec a))
          (let [im-size (long (:im-size spec))
                rows    (long (:rows spec))
                cols    (long (:cols spec))
                ;; im0 (rem (aget a 0) im-size)
                ;; im1 (rem (aget a (dec len)) im-size)
                im0 (rem (.get path 0) im-size)
                im1 (rem (.get path (dec path-length)) im-size)
                x0  (rem  im0 rows)
                y0  (quot im0 rows)
                x1  (rem  im1 rows)
                y1  (quot im1 rows)
                path-angle (if (and (= (- x1 x0) 0) (= (- y0 y1) 0))
                             -666.0
                             (let [a (float (int (+ (rad2deg (Math/atan2 (double (- y0 y1)) (double (- x1 x0))))
                                                    0.5)))]
                               (cond
                                 (=  a  180.0) 0.0
                                 (<= a -180.0) (+ a 360.0)
                                 (<  a  0.0)   (+ a 180.0)
                                 :else a)))] ;; angle should range from 0 - 179 degrees
            (->PathData id (int-array len a) chord-len
                        entry-xy entry-xz exit-xy exit-xz path-angle
                        energy (residue* this x) nil))))))
  ;; (toPathData [this offset]
  ;;   (let [pd ^PathData (->PathData (int-array path-length) chord-len energy)
  ;;         a ^ints (.path pd)]
  ;;     (loop [i ^int (int 0)]
  ;;       (when (< i path-length)
  ;;         (aset a i (unchecked-subtract-int (.get path i) offset))
  ;;         (recur (unchecked-inc-int i))))
  ;;     pd))
  IPathCompute
  (dot* [this x]
    (loop [i (int 0)
           sum 0.0]
      (if (< i path-length)
        (recur (unchecked-inc-int i) (+ sum ^double (x (.get path i))))
        (* sum chord-len))))

  (residue* [this x]
    (- ^double (dot* this x) (double energy))))

(defn HistoryBuffer->fromStream
  "Read the path and b data from two seperate files provided by Paniz, each one contains everything.
   pis : BufferedInputStream for path file
   bis : BufferedInputstream for b file

   Return a HistoryBuffer"

  #_([^java.io.BufferedInputStream is]
   (when-let [[^int length ^bytes len-buf] (read-int is true)]
     (let [path-buf ^bytes (byte-array (* 4 length))
           path ^IntBuffer (-> (ByteBuffer/wrap ^bytes path-buf)
                               (.order ByteOrder/LITTLE_ENDIAN)
                               .asIntBuffer)
           [^double chord-len ^bytes chord-len-buf] (read-double is true)]
       (when-not (= (.read is path-buf) -1)
         (when-let [[^float energy ^bytes e-buf] (read-float is true)]
           (->HistoryBuffer length path energy len-buf path-buf e-buf (HashMap.)))))))
  ([^long id ^java.io.BufferedInputStream pis ^java.io.BufferedInputStream bis]
   (when-let [[^int length ^bytes len-buf] (read-int pis true)]
     (let [path-buf ^bytes (byte-array (* 4 length))
           path ^IntBuffer (-> (ByteBuffer/wrap ^bytes path-buf)
                               (.order ByteOrder/LITTLE_ENDIAN)
                               .asIntBuffer)]
       (when-not (= (.read pis path-buf) -1)
         (let [[^double chord-len   ^bytes chord-len-buf] (read-double pis true)
               [^FloatBuffer angles ^bytes angle-buf]     (read-floats pis 4 true)
               [^float energy       ^bytes e-buf]         (read-float bis true)]
           (->HistoryBuffer id
                         length path
                         chord-len (.get angles 0) (.get angles 1) (.get angles 2) (.get angles 3)
                         energy
                         len-buf path-buf chord-len-buf angle-buf e-buf (HashMap.))))))))

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
           (->PathData id path-arr chord-len
                       (.get angles 0) (.get angles 1) (.get angles 2) (.get angles 3) (float -360.0)
                       energy
                       0.0
                       nil)))))))

(deftype HistoryInputStream [^java.io.BufferedInputStream pis ^java.io.BufferedInputStream bis ^long length
                             ^{:unsynchronized-mutable true :tag long} index
                             ^{:unsynchronized-mutable true :tag boolean} open?]
  IHistoryInput
  (skip* [this k]
    (let [last-idx (+ index ^long k)]
      (if bis
        (loop [i index]
          (if (< i last-idx)
            (when-let [b (HistoryBuffer->fromStream i pis bis)]
              (recur (unchecked-inc i)))
            (set! index i)))
        (loop [i index]
          (if (< i last-idx)
            (when-let [b (HistoryBuffer->fromStream i pis bis)]
              (recur (unchecked-inc i)))
            (set! index i))))
      this))

  (next* [this]
    (when open?
      (let [s (HistoryBuffer->fromStream index pis bis)]
        (set! index (inc index))
        s)))

  (next* [this n]
    (let [acc ^ArrayList (ArrayList. (int n))
          last-idx (+ index ^long n)]
      (loop [i index]
        (if (< i last-idx)
          (when-let [b (HistoryBuffer->fromStream i pis bis)]
            (.add acc b)
            (recur (inc i)))
          (set! index i)))
      acc))

  (next* [this n out-ch]
    (let [last-idx (+ index ^long n)]
      (loop [i index]
        (if (< i last-idx)
          (when-let [b (HistoryBuffer->fromStream i pis bis)]
            (>!! out-ch b)
            (recur (inc i)))
          (set! index i))))
    (a/close! out-ch))

  (rest* [this out-ch] ;; read
    (loop []
      (when-let [b (HistoryBuffer->fromStream index pis bis)]
        (set! index (inc index))
        (>!! out-ch b)
        (recur)))
    (a/close! out-ch))

  (rest* [this out-ch batch-size]
    ;; read in data as Historybuffer
    (let [batch-size ^long batch-size]
      (loop [acc ^objects (object-array batch-size)
             i   ^long    (long 0)
             c index]
        (if-let [b (HistoryBuffer->fromStream c pis bis)]
          (do (if (< i batch-size)
                (do (aset acc i b)
                    (recur acc (unchecked-inc i) (unchecked-inc c)))
                (let [new-acc ^objects (object-array batch-size)]
                  (>!! out-ch [i acc])
                  (aset new-acc 0 b)
                  (recur new-acc (long 1) (unchecked-inc c)))))
          (do (>!! out-ch [i acc])
              (set! index c))))
      (a/close! out-ch)))

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
             i   ^long    (long 0)]
        (if-let [b (PathData->fromStream index pis bis)]
          (if (< i batch-size)
            (do (aset acc i b)
                (recur acc (unchecked-inc i)))
            (let [new-acc ^objects (object-array batch-size)]
              (>!! out-ch acc)
              (aset new-acc 0 b)
              (recur new-acc (long 1))))
          (do (>!! out-ch acc))))
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


(deftype HistoryOutputStream [^String name ^RandomAccessFile raf ^BufferedOutputStream os
                           ^{:unsynchronized-mutable true :tag int} n]
  IHistoryOutput
  (writeHistory* [this history]
    (let [len-buf  ^bytes (.len-buf  ^HistoryBuffer history)
          path-buf ^bytes (.path-buf ^HistoryBuffer history)
          e-buf    ^bytes (.e-buf    ^HistoryBuffer history)]
      (if (and len-buf path-buf e-buf)
        (do
          (.write os len-buf  0 (alength len-buf))
          (.write os path-buf 0 (alength path-buf))
          (.write os e-buf    0 (alength e-buf))
          (set! n (unchecked-inc-int n))
          true)
        false)))
  (writeHeader* [this header]
    (.flush os)
    (let [n ^long (.getFilePointer raf)]
      (.seek raf 0)
      (.writeBytes raf (format (format "%%0%dd\n" header-length) header))
      (when (> n 0)
        (.seek raf n))))

  ICloseable
  (close* [this]
    (.close this))

  java.lang.AutoCloseable
  (close [this]
    ;; (print "closing output stream ...")
    (writeHeader* this n)
    (.close os)
    (.close raf)
    ;; (println "done.")
    ))

(defn newHistoryOutputStream
  ([fname]
   (newHistoryOutputStream fname 8192))
  ([fname size]
   (let [f ^File (File. ^String fname)
         raf ^RandomAccessFile (RandomAccessFile. f "rw")
         bos ^BufferedOutputStream (BufferedOutputStream. (FileOutputStream. (.getFD raf)) (int size))]
     (.writeBytes raf (format (format "%%0%dd\n" header-length) 0))
     (HistoryOutputStream. fname raf bos (int 0)))))


;; (defn read-path-samples [path-file b-file & {:keys [lines skip out] :or {skip 0}}]
;;   (println (format "Reading from file %s for %d lines, skipping %d lines." path-file lines skip))
;;   (with-open [pis ^java.io.BufferedInputStream (io/input-stream path-file)
;;               bis ^java.io.BufferedInputStream (io/input-stream b-file)]
;;     (let [p-count ^int (read-header pis)
;;           b-count ^int (read-header bis)]
;;       (assert (= p-count b-count))
;;       (dotimes [_ skip]
;;         (HistoryBuffer->fromStream pis bis))
;;       (if out
;;         (let [limit (or lines p-count)]
;;           (dotimes [i limit]
;;             (>!! out (HistoryBuffer->fromStream pis bis)))
;;           (a/close! out)
;;           (assert (= (.read pis) -1))
;;           (println (format "%s ends ... OK." path-file))
;;           (assert (= (.read bis) -1))
;;           (println (format "%s ends ... OK." b-file)))
;;         (let [a ^ArrayList (ArrayList.)]
;;           (dotimes [_ (or lines 1000)]
;;             (.add a (HistoryBuffer->fromStream pis bis)))
;;           (let [v (vec a)]
;;             (.clear a)
;;             v))))))

;; This is mean to used in a single thread
(deftype HistoryAccumulator [^ArrayList hist-acc
                             ^{:unsynchronized-mutable true :tag long} total
                             ^{:unsynchronized-mutable true :tag long} chunk
                             ^long chunk-size ^HistoryOutputStream out]
  ;; java.util.Collection
  IAccumulator
  (add* [this hist]
    (set! total (unchecked-add total (count hist)))
    (let [n (unchecked-add chunk (count hist))]
      (if (> n chunk-size)
        (do
          (write-out* this)
          (.clear hist-acc)
          (doseq [b hist]
            (writeHistory* out b))
          (set! chunk 0))
        (do
          (.add hist-acc hist)
          (set! chunk n)))
      this))

  IOutput
  (write-out* [this]
    (doseq [acc hist-acc]
      (doseq [b acc]
        (writeHistory* out b))))

  ICloseable
  (close* [this]
    (.close this))

  java.lang.AutoCloseable
  (close [this]
    (when (> (.size hist-acc) 0)
      (write-out* this)
      (.clear hist-acc)
      (set! chunk 0))
    (writeHeader* out total)
    (.close out))

  clojure.lang.Counted
  (count [_] total)

  clojure.lang.IPersistentVector
  (seq [_] (list (.name out) total chunk)))


(defn newHistoryAcc [filename & {:keys [truncate chunk] :or {truncate false chunk 1000}}]
  (let [f ^File (io/file filename)]
    (println "Filename: " (.getName f))
    (when-let [d ^File (.getParentFile f)]
      (.mkdirs d))
    (when (and (.exists f) (not truncate))
      (backup f))
    (let [fos ^RandomAccessFile (RandomAccessFile. f "rw")]
      (write-header fos 0 :fixed true)
      (HistoryAccumulator. (ArrayList.) 0 0 chunk (newHistoryOutputStream filename (* 1024 1024))))))


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

(deftype HistoryIndex [^ArrayList sliceIndex
                       ^ArrayList voxel-histograms ;; keep track of histogram for each node on index,
                                                   ;; so DROP could be computed independently for each block
                       ^{:unsynchronized-mutable true :tag int} total
                       ;; ^RealBlockVector voxel-histogram
                       ^int rows ^int cols ^int slices]
  Object
  (equals [this other]
    (and (= total (count other))
         (.equals sliceIndex ^ArrayList (.sliceIndex ^HistoryIndex other))))

  IHistoryIndex
  (addHistories* [this hist slice length]
    (ensureSize sliceIndex slice #(ArrayList.))
    (let [s-entry ^ArrayList (.get sliceIndex slice)]
      (ensureSize s-entry length #(ArrayList.))
      (let [e ^ArrayList (.get s-entry length)]
        (.addAll e hist)
        (set! total (unchecked-add-int total (count hist))))))

  (removeHistories* [this i]
    (when (< (int i) (.size sliceIndex))
      (let [slice ^ArrayList (.get sliceIndex i)
            n ^int (reduce unchecked-add-int (int 0) (mapv #(.size ^ArrayList %) slice))]
        (set! total (unchecked-subtract-int total n))
        (.clear slice)
        this)))


  (removeHistories* [this i j]
    (when (< (int i) (.size sliceIndex))
      (let [slice ^ArrayList (.get sliceIndex i)]
        (when (< (int j) (.size slice))
          (let [entry ^ArrayList (.get slice j)]
            (set! total (unchecked-subtract-int total (.size entry)))
            (.clear entry)
            this)))))

  (mergeIndex* [this m]
    (let [idx-it (clojure.lang.RT/iter m)]
      (loop []
        (when (.hasNext idx-it)
          (let [[^int idx ^HashMap length-map] (.next idx-it)
                len-it (clojure.lang.RT/iter length-map)
                sliceIndex-idx ^ArrayList (.get sliceIndex idx)]
            (loop []
              (when (.hasNext len-it)
                (let [[^int len ^ArrayList data] (.next len-it)
                      sliceIndex-idx-len ^ArrayList (.get sliceIndex-idx len)]
                  (.addAll sliceIndex-idx-len data)
                  (set! total (unchecked-add-int total (.size data)))
                  (recur))))
            (recur)))))
    #_(loop [m1 m]
      (when-let [[[^int s ^ArrayList m2] & rst-m1] m1]
        (let [entry ^ArrayList (.get sliceIndex s)]
          (loop [m2 m2]
            (when-let [[[^int l ^ArrayList hist] & rst-m2] m2]
              (ensureSize entry l #(ArrayList.))
              (.addAll ^ArrayList (.get entry l) hist)
              (set! total (unchecked-add-int total (count hist)))
              (recur rst-m2))))
        (recur rst-m1))))

  (summary* [this]
    (let [N (count sliceIndex)]
      (loop [i 0]
        (when (< i N)
          (print (format "%2d : " i))
          (let [slice ^ArrayList (.get sliceIndex i)
                K (count slice)]
            (loop [j 0]
              (when (< j K)
                (print (format " [%d] " (count (.get slice j))))
                (recur (unchecked-inc j)))))
          (println "")
          (recur (unchecked-inc i))))
      (println "Total : " total)))

  (trimToSize* [this]
    (.trimToSize sliceIndex)
    (let [n_s ^int (.size sliceIndex)]
      (loop [i (int 0)]
        (if (< i n_s)
          (let [slice ^ArrayList (.get sliceIndex i)
                n_l ^int (.size slice)]
            (loop [k (int 0)]
              (when (< k n_l)
                (let [e ^ArrayList (.get slice k)]
                  (.trimToSize e))
                (recur (unchecked-inc-int k))))
            (recur (unchecked-inc-int i)))
          this))))

  (branch-seq* [this]
    ())


  clojure.lang.IPersistentVector
  (count [_] total)
  (seq [_]
    (let [a ^ArrayList (.get sliceIndex 0)
          m (.size a)]
      (hist-seq sliceIndex (.size sliceIndex) a m 0 0)))

  clojure.lang.IFn
  (invoke [this i j]
    (when (< (int i) (.size sliceIndex))
      (let [slice ^ArrayList (.get sliceIndex i)]
        (when (< (int j) (.size slice))
          (.get slice j)))))
  (invoke [this i]
    (when (< (int i)  (.size sliceIndex))
      (.get sliceIndex (int i))))
  (invoke [this]
    (let [n ^int (.size sliceIndex)]
      (loop [m (sorted-map)
             i (int 0)]
        (if (< i n)
          (let [slice ^ArrayList (.get sliceIndex i)
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
      (throw (UnsupportedOperationException.))))

  IVoxelHistogram
  (reset-hit-counts* [this]
    (dotimes [i (.size voxel-histograms)]
      (let [arr ^ArrayList (.get voxel-histograms i)]
        (dotimes [j (.size arr)]
          (.set arr j nil))))
    nil)

  (counted?* [_]
    (not (every? #(every? nil? %) voxel-histograms)))

  (count-voxel-hits* [this] (count-voxel-hits* this {:forced false
                                                     :jobs (- ^int pct.util.system/PhysicalCores 4)
                                                     :batch-size 250000}))

  (count-voxel-hits* [this opts]
    (let [{forced     :forced
           jobs       :jobs
           batch-size :batch-size,
           :or {forced false jobs (- ^int pct.util.system/PhysicalCores 4) batch-size 250000}} opts]
      (timbre/info (format "Counting voxel hits: [:forced %b, :jobs %d, batch-size %d]" forced jobs batch-size))
      (if forced
        (reset-hit-counts* this))
      (when-not (counted?* this)
        #_(let [from (a/chan jobs)
                to   (a/chan jobs)]
            ;; (a/onto-chan  from (take 100 s))
            (a/go-loop [s (seq this)
                        t nil
                        start (int -1)
                        len   (int -1)]
              (cond
                (nil? s)
                (a/close! from)

                t
                (let [head (take batch-size t)
                      tail (drop batch-size t)]
                  ;; (timbre/info (format "Sending histories (1) [%d %d %d] ..." (count head) start len))
                  (if (a/>! from [head start len])
                    (if (empty? tail)
                      (recur (next s) nil (int -1) (int -1))
                      (recur s tail start len))
                    (a/close! from)))

                (= (count (nth (first s) 0)) 0)
                (recur (next s) nil (int -1) (int -1))

                :else
                (let [[branch start len] (first s)
                      head (take batch-size branch)
                      tail (drop batch-size branch)]
                  ;; (timbre/info (format "Sending histories (2) [%d %d %d] ..." (count head) start len))
                  (if (a/>! from [head start len])
                    (if (empty? tail)
                      (recur (next s) nil (int -1) (int -1))
                      (recur s tail start len))
                    (a/close! from))))
              #_(if (and s (>! from (first s)))
                  (recur (next s))
                  (a/close! from)))
            (a/pipeline-async jobs to
                              (fn [data-chunk res]
                                (a/go (let [[data start len] data-chunk
                                            n (count data)
                                            #_#__ (timbre/info (format "Start counting voxels for %d histories [%d %d]" n start len))
                                            v-hist (voxel-count-batch data rows cols slices)]
                                        #_(timbre/info (format "Finished counting voxels for %d histories" n))
                                        (>! res [v-hist start len])
                                        (a/close! res))))
                              from)
            (let [v-hits (blocking-reduce (fn [^ArrayList acc a]
                                            (let [[^RealBlockVector v-hist ^int start ^int len] a
                                                  entry ^ArrayList (.get acc start)]
                                              #_(timbre/info (format "Updating branch [%d, %d]" start len))
                                              (when (>= len (.size entry))
                                                (timbre/info "Sizing up the entry" start "to" (inc len))
                                                (ensureSize entry (inc len) (fn [] nil)))
                                              (if-let [branch ^RealBlockVector (.get entry len)]
                                                (do (axpy! v-hist branch)
                                                    (release v-hist))
                                                (.set entry len v-hist))
                                              acc))
                                          voxel-histograms
                                          ;; (dv (* rows cols slices))
                                          to)]
              (timbre/info "Voxel hit counting finished.")
              this))
        (let [data-ch (a/chan jobs)
              res-ch (pct.async.threads/asyncWorkers
                      jobs
                      (fn [[data start-idx len]]
                        [(voxel-count-batch data rows cols slices)  start-idx len])
                      (fn
                        ([] voxel-histograms)
                        ([acc] acc)
                        ([^ArrayList acc [^RealBlockVector v-hist ^int start-idx ^int len]]
                         #_(timbre/info (format "Updating branch [%d, %d]" start-idx len))
                         (let [branch ^ArrayList (.get acc start-idx)]
                           (when (>= len (.size branch))
                             (timbre/info "Sizing up the branch" start-idx "to" (inc len))
                             (ensureSize entry (inc len) (fn [] nil)))
                           (if-let [h ^RealBlockVector (.get branch len)]
                             (do (axpy! v-hist h)
                                 (release v-hist))
                             (.set branch len v-hist))
                           acc)))
                      data-ch)]
          (let []
            ;; dispatch
            (try
              (loop [start-idx ^long (long 0)]
                (if (< start-idx slices)
                  (let [branch ^ArrayList (.get sliceIndex start-idx)
                        branch-len ^long  (long (.size branch))]
                    (loop [len ^long (long 0)]
                      (if (< len branch-len)
                        (let [data  (.get branch len)
                              parts (partition batch-size data)
                              it    (clojure.lang.RT/iter parts)]
                          (loop []
                            (when (.hasNext it)
                              (a/>!! data-ch [(.next it) start-idx len])
                              (recur)))
                          (recur (unchecked-inc len)))))
                    (recur (unchecked-inc start-idx)))
                  (a/close! data-ch)))
              (catch Exception ex
                (a/close! data-ch)
                (timbre/error ex "[count-voxel-hits*] Something went wrong during dispatch." (.getName (Thread/currentThread)))))
            (a/<!! res-ch)
            this))
        #_(count-voxels voxel-histograms rows cols slices :jobs jobs :batch-size batch-size))))


  (getVoxelCount* [this i]
    (when (< ^long i (.size voxel-histograms))
      (when-let [s ^ArrayList (.get voxel-histograms i)]
        (reduce (fn [^RealBlockVector acc ^RealBlockVector v]
                  (if v
                    (axpy! v acc)
                    acc))
                (dv (* rows cols slices))
                s))))

  (getVoxelCount* [this i j]
    (when (< ^long i (.size voxel-histograms))
      (when-let [s ^ArrayList (.get voxel-histograms i)]
        (when (< ^long j (.size s))
          (.get s j))))))


(defn createHistoryIndex
  ([{^long rows :rows, ^long cols :cols, ^long slices :slices, :as spec}]
   (let [a  ^ArrayList (ArrayList. slices) ;; index
         vh ^ARrayList (ArrayList. slices) ;; voxel hits
         ]
     (dotimes [i slices]
       (.add a  (ArrayList. ^Collection (vec (repeatedly (int (inc (- slices i))) #(ArrayList.)))))
       (.add vh (ArrayList. ^Collection (vec (repeat     (int (inc (- slices i))) nil)))))
     (->HistoryIndex a vh 0 #_(dv (* ^int rows ^int cols ^int slices)) rows cols slices)))
  ([^long rows ^long cols ^long slices]
   (let [slices ^int (int slices)
         a  ^ArrayList (ArrayList. slices)
         vh ^ARrayList (ArrayList. slices)]
     (dotimes [i slices]
       (.add a  (ArrayList. ^Collection (vec (repeatedly (int (inc (- slices i))) #(ArrayList.)))))
       (.add vh (ArrayList. ^Collection (vec (repeat     (int (inc (- slices i))) nil)))))
     (->HistoryIndex a vh 0 #_(dv (* ^int rows ^int cols ^int slices)) rows cols slices))))



#_(defprotocol IHistoryIndex
    (addHistory*  [this b] "Add history to ith slice, kth entry")
    (getHistorys* [this i k])
    (mergeIndex* [this m])
    (index* [this] "return a key map"))

#_(deftype HistoryIndex [^HashMap sliceIndex
                      ^{:unsynchronized-mutable true :tag int} total]
    Object
    (equals [this other]
      (.equals sliceIndex ^HashMap (.sliceIndex ^HistoryIndex other)))

    IHistoryIndex
    (when (empty? (.slices ^HistoryBuffer history))
      (tag-sliceIDs* ^HistoryBuffer history 40000))
    (when-not (empty? (.slices ^data.pct.HistoryBuffer history))
      (let [ks (sort (keys (.slices ^data.pct.HistoryBuffer history)))
            idx (first ks)]
        (if-let [s ^HashMap (.get sliceIndex idx)]
          (if-let [bs ^ArrayList (.get s (count ks))]
            (.add bs history)
            (let [a ^ArrayList (ArrayList.)]
              (.add a history)
              (.put s (count ks) a)))
          (let [m ^HashMap (HashMap.)
                a ^ArrayList (ArrayList.)]
            (.add a history)
            (.put m (count ks) a)
            (.put sliceIndex (int idx) m)))
        (set! total (unchecked-inc-int total))
        this))

    (getHistorys* [this i k]
      (when-let [s ^HashMap (.get sliceIndex i)]
        (when-let [bs ^ArrayList (.get s k)]
          bs)))

    (mergeIndex* [this m]
      (loop [m ^HashMap (.sliceIndex ^HistoryIndex m)]
        (when-let [[[i ^HashMap s] & rst-m] m]
          (if-let [slice ^HashMap (.get sliceIndex i)]
            (loop [s s]
              (when-let [[[k coll] & rst-s] s]
                (if-let [bs ^ArrayList (.get slice k)]
                  (.addAll bs coll)
                  (.put slice (int k) coll))
                (recur rst-s)))
            (.put sliceIndex (int i) s))
          (recur rst-m)))
      (set! total (unchecked-add-int total (count m))))

    (index* [this]
      (reduce (fn [acc [k s]]
                (assoc acc k (into (sorted-set slices) (keys s))))
              (sorted-map)
              sliceI slices valAt [this idx] (.valAt this idx nil))
    (valAt [this idx not-found]
      (let [[s-id len] idx]
        (if-let [s ^HashMap (.get sliceIndex (int s-id))]
p          (if len
            (.getOrDefault s (int len) not-found)
            s)
          not-found)))
    clojure.lang.IPersistentMap
    (count [this] total)
    (containsKey [this ks]
      (if (.valAt this ks) true false))))

#_(defn createHistoryIndex []
  (HistoryIndex. (HashMap.) 0))

