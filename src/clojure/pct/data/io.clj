(ns pct.data.io
  (:use clojure.core)
  (:require pct.util.system
            pct.data
            [pct.async.threads :refer [asyncWorkers]]
            [clojure.java.io :as io]
            [clojure.string  :as s]
            [clojure.core.async :as a :refer [>! <! >!! <!! chan]]
            [taoensso.timbre :as timbre]
            [uncomplicate.neanderthal
             [core   :refer :all]
             [native :refer :all]])
  (:import [java.nio ByteOrder ByteBuffer IntBuffer]
           [java.io FileOutputStream BufferedOutputStream BufferedInputStream RandomAccessFile File]
           [java.util HashMap ArrayList]))



(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defonce header-length ^int (count (str (Integer/MAX_VALUE))))


(defn backup [f]
  (let [fname (.getName ^File f)
        f2 ^File (File. (format "%s.old" fname))]
    (if (.exists f2)
      (loop [n 1]
        (let [f3 ^File (File. (format "%s.old.%d" fname n))]
          (if (.exists f3)
            (recur (inc n))
            (do
              (println (.getName f3))
              (.renameTo ^File f f3)))))
      (.renameTo ^File f f2))))

(defn backupFile [file]
  (let [name (.getName ^File file)
        old ^File (File. (format "%s.old" name))]
    (when (.exists old)
      (letfn [(fix-name [curr ^long n]
                (let [target ^File (File. (format "%s.old.%d" name n))]
                  (if (.exists target)
                    (do
                      (fix-name target (inc n))
                      (.renameTo ^File curr target))
                    (.renameTo ^File curr target))))]
        (fix-name old 1)))
    (.renameTo ^File file old)))


#_(defn renameFile2
  "The same thing, but using loop instead of recursion"
  [file]
  (let [name (.getName ^File file)
        old ^File (File. (format "%s.old" name))]
    (when (.exists old)
      (let [flist (loop [n 1
                         flist '()]
                    (let [f (File. (format "%s.old.%d" name n))]
                      #_(println (.getName f))
                      (if (.exists ^File f)
                        (recur (inc n) (conj flist f))
                        (conj flist f))))]
        (loop [flist flist]
          (when-let [[a & rst] flist]
            #_(println flist)
            (if-let [b (first rst)]
              (do
                (.renameTo ^File b ^File a)
                (recur rst))
              (.renameTo ^File old ^file a))))))
    (.renameTo ^File file old)))

(defmacro read-int
  ([is] `(read-int ~is nil))
  ([is save-buf]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag "bytes"}) (byte-array 4)]
        (when (not= (.read ~is ~buffer) -1)
          ~(if save-buf
             `[(int (-> (ByteBuffer/wrap ~buffer)
                        (.order ByteOrder/LITTLE_ENDIAN)
                        (.getInt)))
               ~buffer]
             `(int (-> (ByteBuffer/wrap ~buffer)
                       (.order ByteOrder/LITTLE_ENDIAN)
                       (.getInt)))))))))


(defmacro write-int [os]
  (let [buffer (gensym)]
    `(let [~(with-meta buffer {:tag "bytes"}) (byte-array 4)]
       (.write ~os ~buffer)
       [(int (-> (ByteBuffer/wrap ~buffer)
                 (.order ByteOrder/LITTLE_ENDIAN)
                 (.getInt)))
        ~buffer])))

(defmacro read-float
  ([is] `(read-float ~is nil))
  ([is save-buf]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag "bytes"}) (byte-array 4)]
        (when (not= (.read ~is ~buffer) -1)
          ~(if save-buf
             `[(float (-> (ByteBuffer/wrap ~buffer)
                          (.order ByteOrder/LITTLE_ENDIAN)
                          (.getFloat)))
               ~buffer]
             `(float (-> (ByteBuffer/wrap ~buffer)
                         (.order ByteOrder/LITTLE_ENDIAN)
                         (.getFloat))))))))
  #_`(let [buffer# (byte-array 4)]
     (.read ~is buffer#)
     [(float (-> (ByteBuffer/wrap buffer#)
                 (.order ByteOrder/LITTLE_ENDIAN)
                 (.getFloat)))
      buffer#]))

(defmacro read-floats
  ([is n] `(read-float ~is ~n nil))
  ([is n save-buf]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag "bytes"}) (byte-array (* 4 ~n))]
        (when (not= (.read ~is ~buffer) -1)
          ~(if save-buf
             `[(-> (ByteBuffer/wrap ~buffer)
                   (.order ByteOrder/LITTLE_ENDIAN)
                   (.asFloatBuffer))
               ~buffer]
             `(-> (ByteBuffer/wrap ~buffer)
                  (.order ByteOrder/LITTLE_ENDIAN)
                  (.asFloatBuffer)))))))
  #_`(let [buffer# (byte-array 4)]
     (.read ~is buffer#)
     [(float (-> (ByteBuffer/wrap buffer#)
                 (.order ByteOrder/LITTLE_ENDIAN)
                 (.getFloat)))
      buffer#]))

(defmacro read-double
  ([is] `(read-double ~is nil))
  ([is save-buf]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag "bytes"}) (byte-array 8)]
        (when (not= (.read ~is ~buffer) -1)
          ~(if save-buf
             `[(double (-> (ByteBuffer/wrap ~buffer)
                           (.order ByteOrder/LITTLE_ENDIAN)
                           (.getDouble)))
               ~buffer]
             `(double (-> (ByteBuffer/wrap ~buffer)
                          (.order ByteOrder/LITTLE_ENDIAN)
                          (.getDouble)))))))))

#_(defmacro read-double [is]
  `(let [buffer# (byte-array 8)]
     (.read ~is buffer#)
     [(double (-> (ByteBuffer/wrap buffer#)
                  (.order ByteOrder/LITTLE_ENDIAN)
                  (.getDouble)))
      buffer#]))


(defmacro int-buffer [i]
  `(let [buf# ^bytes (byte-array 4)]
     (.put ^IntBuffer (-> (ByteBuffer/wrap buf#)
                          (.order ByteOrder/LITTLE_ENDIAN)
                          .asIntBuffer)
           0 ~i)
     buf#))

(defn ^bytes int-array-buffer [^ints arr]
  (let [len ^int (alength arr)
        buf ^bytes (byte-array (* 4 len))
        byte-buf ^IntBuffer (-> (ByteBuffer/wrap buf)
                                 (.order ByteOrder/LITTLE_ENDIAN)
                                 .asIntBuffer)]
     (loop [i ^int (int 0)]
       (if (< i len)
         (do
           (.put byte-buf i (aget arr i))
           (recur (unchecked-inc-int i)))
         buf))))


(defn read-header
  "Header format: number of histories is store as string, followed with a new line (0x0a)"
  [^java.io.BufferedInputStream is & {:keys [size]
                                      :or   {size (inc ^int header-length)}}]
  (let [end-idx (unchecked-dec-int size)]
    (loop [c      ^byte  (unchecked-byte (.read is))
           header ^bytes (byte-array size)
           idx    ^int   (int 0)]
      (if (or (>= idx end-idx)
              (= c (byte 10))) ;; (byte 10) is \n
        (Integer/parseInt (String. header 0 idx))
        (do
          (aset header idx c)
          (recur (unchecked-byte (.read is))
                 header
                 (unchecked-inc-int idx)))))))


(defn write-header
  "Header format: number of histories is store as string, followed with a new line (0x0a)
   "
  [^java.io.RandomAccessFile os header & {:keys [fixed size]
                                          :or {fixed true size header-length}}]
  (let [curr ^long (.getFilePointer os)]
    (.seek os 0)
    (if fixed
      (.writeBytes os  (format (format "%%0%dd\n" header-length) header))
      (.writeBytes os  (format "%d\n" header)))
    (when (> curr 0)
      (.seek os curr))))



;; (defn initReconFromFile
;;   ([& {:keys [rows cols] :or {rows 200 cols 200}}]
;;    (let [f "/local/cair/data_4_Ritchie/exp_CTP404/B_1280000_L_1.000000_359/x_0_0.txt"
;;          x (dv 40000)]
;;      (with-open [rdr (io/reader f)]
;;        (loop [lines (line-seq rdr)]
;;          (when-let [[ln & rst] lines]
;;            (println ln)
;;            (recur rst)))))))


#_(defn load-vctr [filename vctr rows cols]
    (assert (= (dim vctr) (* rows cols)))
    (with-open [rdr (io/reader filename)]
      (loop [lines (line-seq rdr)
             offset (long 0)]
        (if-let [[ln & rst] lines]
          (let [str-vals (s/split ln #"\s+")
                n (count str-vals)]
            (assert (= cols n))
            (dotimes [i n]
              (vctr (unchecked-add i offset)  (java.lang.Float/parseFloat (nth str-vals i)))
              #_(vctr (unchecked-add i offset) (unchecked-add i offset) ))
            (recur rst (unchecked-add offset (long rows))))
          vctr))))

(defn load-vctr
  ([filename vctr]
   ;; (println "load-vctr2 v0.2")
   (with-open [rdr (io/reader filename)]
     (let [str-vals (s/split (slurp rdr) #"\s+")]
       (assert (= (dim vctr) (count str-vals)))
       (reduce-kv (fn [acc k v]
                    (acc k (java.lang.Float/parseFloat v))
                    acc)
                  vctr
                  str-vals)))))

(defn load-series [prefix & {:keys [rows cols slices ext iter] :or {rows 200 cols 200 slices 16 ext "txt" iter 0}}]
  ;;Verify files
  (dotimes [i slices]
    (let [fname (format "%s_%d_%d.%s" prefix iter i ext)]
      (if (.exists ^File (File. fname))
        (timbre/info (format "%s ... OK." fname ))
        (java.io.FileNotFoundException. (format "%s not found" fname)))))

  (let [length (* (long rows) (long cols))
        x (dv (* (long length) (long slices)))]
    (loop [offset (long 0)
           i (long 0)]
      (if (< i (long slices))
        (let [fname (format "%s_%d_%d.%s" prefix iter i ext)
              sv (subvector x offset length)]
          (load-vctr fname sv)
          (recur (+ offset  length) (unchecked-inc-int i)))
        x))))



(defn save-x [x prefix & {:keys [id rows cols ext] :or {id 0 rows 200 cols 200 ext "txt"}}]
  (let [fname (format "%s_%d.%s" prefix id ext)
        file ^File (java.io.File. fname)
        z ^float (float 0)]
    (assert (= (dim x) (* (long rows) (long cols))))
    (when-let [d ^File (.getParentFile file)]
      (.mkdirs d))
    (let [fmt ^java.text.DecimalFormat (java.text.DecimalFormat. "0.#######")]
      (with-open [w (clojure.java.io/writer fname :append false)]
        (loop [s-vals (partition rows (map #(if (= % z) "0" (.format fmt %)) x))]
          (when-let [[line & rst] s-vals]
            (.write w (format "%s \n" (clojure.string/join " " line)))
            (recur rst)))))))


(defn save-x2 [x fname & {:keys [rows cols] :or {rows 200 cols 200}}]
  (let [file ^File (java.io.File. ^String fname)
        z ^float (float 0)]
    (assert (= (dim x) (* (long rows) (long cols))))
    (when-let [d ^File (.getParentFile file)]
      (.mkdirs d))
    (let [fmt ^java.text.DecimalFormat (java.text.DecimalFormat. "0.#######")]
      (with-open [w (clojure.java.io/writer fname :append false)]
        (loop [s-vals (partition rows (map #(if (= % z) "0" (.format fmt %)) x))]
          (when-let [[line & rst] s-vals]
            (.write w (format "%s \n" (clojure.string/join " " line)))
            (recur rst)))))))

(defn save-series [x prefix & {:keys [rows cols ext binary filename]
                               :or   {rows 200 cols 200 ext "txt" binary false}}]
  (let [length (* ^long rows ^long cols)
        len ^long (dim x)]
    (if binary
      ;; Write image as a binary file
      ;; start with rows, cols, slices stored as integer, machine format
      ;; followed by x, stored as double array machine format
      (let [slices (/ len length)
            fname (format "%s/%s" prefix (or filename "x.bin"))
            file ^File (java.io.File. fname)]
        (when-let [d ^File (.getParentFile file)]
          (.mkdirs d))
        (with-open [os (clojure.java.io/output-stream fname)]
          (let [rows-buff   (ByteBuffer/allocate 4)
                cols-buff   (ByteBuffer/allocate 4)
                slices-buff (ByteBuffer/allocate 4)
                dbuff       (ByteBuffer/allocate (* len 8))]
            (-> (.order rows-buff ByteOrder/LITTLE_ENDIAN)
                .asIntBuffer
                (.put ^int rows))
            (-> (.order cols-buff ByteOrder/LITTLE_ENDIAN)
                .asIntBuffer
                (.put ^int cols))
            (-> (.order slices-buff ByteOrder/LITTLE_ENDIAN)
                .asIntBuffer
                (.put ^int slices))
            (.write os (.array rows-buff))
            (.write os (.array cols-buff))
            (.write os (.array slices-buff))
            (let [buf ^DoubleBuffer (-> (.order dbuff ByteOrder/LITTLE_ENDIAN)
                                         .asDoubleBuffer)]
              (loop [i (long 0)]
                (if (< i len)
                  (do (.put buf i (x i))
                      (recur (inc i)))
                  (.write os (.array dbuff))))))))
      ;; Write each slice as text image
      (loop [offset (int 0)
             i (int 0)]
        (when (< offset len)
          (save-x (subvector x offset length) prefix :id i :rows rows :cols cols :ext ext)
          (recur (unchecked-add-int offset length) (unchecked-inc-int i)))))))


#_(defn dumpToMatlab [file x history lambda]
  (let [f (if (instance? java.io.File file ))]))

(defrecord PCTDataset [^long rows ^long cols ^long slices
                       ^java.lang.String base-dir ^java.lang.String path-file ^java.lang.String b-file x0 in-stream])

(defn newPCTDataset [{:keys [rows :rows cols :cols slices :slices dir :dir path :path b :b] :as input}]
  (let [path-file (format "%s/%s" dir path)
        b-file (format "%s/%s" dir b)]
    ;; (println input)
    (map->PCTDataset {:rows rows :cols cols :slices slices
                      :path-file path-file
                      :b-file b-file
                      :in-stream (pct.data/newHistoryInputStream path-file b-file)
                      :x0 (pct.data.io/load-series (format "%s/x" dir)
                                                   :rows rows :cols cols :slices slices :ext "txt" :iter 0)})))


(defn load-dataset [^PCTDataset dataset opts]
  (let [jobs       (long (or (:jobs opts) (- ^int pct.util.system/PhysicalCores 4)))
        min-len    (long (or (:min-len opts) 0))
        batch-size (long (or (:batch-size opts) 20000))
        in-ch      (a/chan jobs)
        out-ch     (vec (repeatedly jobs #(a/chan (int (/ jobs 2)))))
        init-x     (:x0 dataset)
        offset     (* ^long (:rows dataset) ^long (:cols dataset))]
    (timbre/info (format "Loading dataset with %d workers, batch size = %d" jobs batch-size))
    (let [res-ch     (pct.async.threads/asyncWorkers
                      jobs
                      (fn [^objects bulk-data]
                        (let [len ^long (long (alength bulk-data))
                              acc ^HashMap (HashMap.)]
                          (loop [i (long 0)]
                            (if (< i len)
                              (let [history ^pct.data.HistoryBuffer (aget bulk-data i)]
                                (when (pct.data/long-enough?*  history min-len)
                                  (pct.data/tag-sliceIDs*  history offset)
                                  (when-not (empty? (.slices history))
                                    (let [ks (vec (sort (keys (.slices history))))
                                          idx (int (ks 0))
                                          last-slice (int (ks (dec (count ks))))
                                          len (inc (- last-slice idx))]
                                      ;; (timbre/info (format "idx, len = [%d, %d]" idx len))
                                      (when (not= len (count ks))
                                        (timbre/info "Warning: Path has skipped slice(s)!! --> " ks))
                                      (if-let [s ^HashMap (.get acc idx)]
                                        (if-let [bs ^ArrayList (.get s len)]
                                          (.add bs (pct.data/toPathData history init-x))
                                          #_(.add bs (toPathData history (* offset idx)))
                                          (let [a ^ArrayList (ArrayList.)]
                                            (.add a (pct.data/toPathData history init-x))
                                            #_(.add a (toPathData history (* offset idx)))
                                            (.put s len a)))
                                        (let [m ^HashMap (HashMap.)
                                              a ^ArrayList (ArrayList.)]
                                          (.add a (pct.data/toPathData history init-x))
                                          #_(.add a (toPathData history (* offset idx)))
                                          (.put m len a)
                                          (.put acc idx m))))))
                                (recur (unchecked-inc i)))
                              acc))))
                      (fn
                        ([] (pct.data/createHistoryIndex (:rows dataset) (:cols dataset) (:slices dataset)))
                        ([acc] acc)
                        ([^pct.data.HistoryIndex acc ^HashMap m]
                         (pct.data/mergeIndex* acc m)
                         (.clear m)
                         acc))
                      in-ch)
          res (pct.common/with-out-str-data-map
                (time (do (timbre/info "Start indexing ....")
                          (pct.data/rest* (:in-stream dataset) in-ch batch-size)
                          (let [index (a/<!! res-ch)]
                            (timbre/info "Finished making index." )
                            index))))]
      (println "here?")
      (timbre/info (clojure.string/replace (:str res) #"[\n\"]" ""))
      ;; res
      (pct.data/count-voxel-hits*  (:ans res) {:forced true :jobs jobs :batch-size batch-size}))))
