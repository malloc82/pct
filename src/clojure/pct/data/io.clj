(ns pct.data.io
  (:use clojure.core)
  (:require pct.util.system
            pct.data
            pct.common
            [pct.async.threads :refer [asyncWorkers]]
            [com.rpl.specter :as spr]
            [clojure.java.io :as io]
            [clojure.string  :as s]
            [clojure.core.async :as a :refer [>! <! >!! <!! chan]]
            ;; [clojure.data.json :as json]
            [cheshire.core :refer :all]
            [taoensso.timbre :as timbre]
            [uncomplicate.neanderthal
             [core   :refer :all]
             [native :refer :all]])
  (:import [pct.data ProtonHistory]
           [java.nio ByteOrder ByteBuffer IntBuffer]
           [java.io FileOutputStream FileInputStream BufferedOutputStream BufferedInputStream RandomAccessFile File]
           [java.util HashMap ArrayList Scanner Arrays]
           [java.awt.image BufferedImage Raster WritableRaster]
           [javax.imageio ImageIO]
           [uncomplicate.neanderthal.internal.host.buffer_block IntegerBlockVector RealBlockVector]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

#_(defonce header-length ^int (count (str (Integer/MAX_VALUE))))

(defn ^:private sseq [s fnext]
  (lazy-seq
   (let [b (fnext s)]
     (if b
       (cons b (sseq s fnext))
       '()))))


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


(def ^{:private true :tag 'bytes} __int_buffer__    (byte-array 4))
(def ^{:private true :tag 'bytes} __long_buffer__   (byte-array 8))
(def ^{:private true :tag 'bytes} __float_buffer__  (byte-array 4))
(def ^{:private true :tag 'bytes} __double_buffer__ (byte-array 8))
(def ^{:private true :tag 'long}  __header_length__ 32)
(def ^{:private true :tag 'int}   __max_intersections__ (int 1280)) ;; according to pCT_reconstruction code base
(def ^{:private true :tag 'bytes}     __path_buffer__   (byte-array (* 4 __max_intersections__))) ;; 1280 is the max intersections
(def ^{:private true :tag 'ByteOrder} __ENDIAN__ ByteOrder/LITTLE_ENDIAN)
(def ^{:private true :tag 'long}  __IO_buffer_size__ (long (* 16 1024 1024)))


(defmacro read-int
  "Read an integer from an input stream 'is'
   If a byte buffer is provided, use the given buffer, otherwise use a local created buffer"
  ([is ^ByteOrder endian]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag bytes}) (byte-array 4)]
        (read-int ~is ~endian ~buffer))))
  ([is ^ByteOrder endian ^bytes buffer]
   `(if (= (.read ~is ~buffer) -1)
      (throw (java.io.IOException. "Error: Unexpected EOF when trying to read int"))
      (int (-> (ByteBuffer/wrap ~buffer)
               (.order ~endian)
               (.getInt))))))


#_(defmacro read-ints
  [is endian array]
  (let [buffer (gensym)
        arr    (gensym)]
    `(let [~(with-meta arr {:tag ints}) ~array
           ~(with-meta buffer {:tag bytes}) (byte-array (* 4 (alength ~arr)))]
       (if (= (.read ~is ~buffer) -1)
         (throw (java.io.IOException. (format "Unexpected EOF when reading int-array with length=%d" (alength ~arr))))
         (do (-> (ByteBuffer/wrap ~buffer)
                 (.order ~endian)
                 .asIntBuffer
                 (.get ~arr))
             ~arr)))))


(defmacro read-ints
  ([is endian array]
   (let [buffer (gensym)
         arr    (gensym)]
     `(let [~(with-meta arr {:tag ints}) ~array
            ~(with-meta buffer {:tag bytes}) (byte-array (* 4 (alength ~arr)))]
        (if (= (.read ~is ~buffer) -1)
          (throw (java.io.IOException. (format "Unexpected EOF when reading int-array with length=%d" (alength ~arr))))
          (do (-> (ByteBuffer/wrap ~buffer)
                  (.order ~endian)
                  .asIntBuffer
                  (.get ~arr))
              ~arr)))))

  ([is endian n buffer]
   (let [arr (gensym)
         len (gensym)
         buf (gensym)]
     `(let [~(with-meta len {:tag int})   ~n
            ~(with-meta arr {:tag ints})  (int-array ~len)
            ~(with-meta buf {:tag bytes}) ~buffer]
        (if (= (.read ~is ~buf 0 (* 4 ~len)) -1)
          (throw (java.io.IOException. (format "Unexpected EOF when reading int-array with length=%d" ~len)))
          (do (-> (ByteBuffer/wrap ~buf)
                  (.order ~endian)
                  .asIntBuffer
                  (.get ~arr 0 ~len))
              ~arr))))))


(defmacro read-long
  "Read an long from an input stream 'is'
   If a byte buffer is provided, use the given buffer, otherwise use a local created buffer"
  ([is ^ByteOrder endian]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag bytes}) (byte-array 8)]
        (read-long ~is ~endian ~buffer))))
  ([is ^ByteOrder endian ^bytes buffer]
   `(if (= (.read ~is ~buffer) -1)
      (throw (java.io.IOException. "Error: Unexpected EOF when trying to read long"))
      (long (-> (ByteBuffer/wrap ~buffer)
                (.order ~endian)
                (.getLong))))))


(defmacro read-float
  "Read an float from an input stream 'is'
   If a byte buffer is provided, use the given buffer, otherwise use a local created buffer"
  ([is ^ByteOrder endian]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag bytes}) (byte-array 4)]
        (read-float ~is ~endian ~buffer))))
  ([is ^ByteOrder endian ^bytes buffer]
   `(if (= (.read ~is ~buffer) -1)
      (throw (java.io.IOException. "Error: Unexpected EOF when trying to read float"))
      (float (-> (ByteBuffer/wrap ~buffer)
                 (.order ~endian)
                 (.getFloat))))))


(defmacro read-floats
  [is endian array]
  (let [buffer (gensym)
        arr    (gensym)]
    `(let [~(with-meta arr {:tag floats}) ~array
           ~(with-meta buffer (:tag bytes)) (byte-array (* 4 (alength ~arr)))]
       (if (= (.read ~is ~buffer) -1)
         (throw (java.io.IOException. (format "Unexpected EOF when reading float-array with length=%d" (alength ~arr))))
         (do (-> (ByteBuffer/wrap ~buffer)
                 (.order ~endian)
                 .asFloatBuffer
                 (.get ~arr))
             ~arr)))))


(defmacro read-double
  "Read an double from an input stream 'is'
   If a byte buffer is provided, use the given buffer, otherwise use a local created buffer"
  ([is ^ByteOrder endian]
   (let [buffer (gensym)]
     `(let [~(with-meta buffer {:tag bytes}) (byte-array 8)]
        (read-double ~is ~endian ~buffer))))
  ([is ^ByteOrder endian ^bytes buffer]
   `(if (= (.read ~is ~buffer) -1)
      (throw (java.io.IOException. "Error: Unexpected EOF when trying to read double"))
      (double (-> (ByteBuffer/wrap ~buffer)
                  (.order ~endian)
                  (.getDouble))))))


(defn read-header
  "Header format: number of histories is store as string, followed with a new line (0x0a)"
  (^long [^java.io.BufferedInputStream is]
   (read-header is __header_length__))
  (^long [^java.io.BufferedInputStream is ^long buf-size]
   (let [header (byte-array buf-size)
         __newline__ (byte 10)]
     (loop [i (long 0)]
       (if (< i buf-size)
         (let [c (byte (.read is))]
           (if (= c __newline__)
             (Long/parseLong (String. header 0 i))
             (do (aset header i c)
                 (recur (unchecked-inc i)))))
         (throw (java.io.IOException.
                 (format "Error: Unexpected end when reading header. Header buffer might be too small (%d), header = %s"
                         buf-size (String. header 0 i)))))))))



;; (defmacro int-buffer [i]
;;   `(let [buf# ^bytes (byte-array 4)]
;;      (.put ^IntBuffer (-> (ByteBuffer/wrap buf#)
;;                           (.order ByteOrder/LITTLE_ENDIAN)
;;                           .asIntBuffer)
;;            0 ~i)
;;      buf#))

;; (defn ^bytes int-array-buffer [^ints arr]
;;   (let [len ^int (alength arr)
;;         buf ^bytes (byte-array (* 4 len))
;;         byte-buf ^IntBuffer (-> (ByteBuffer/wrap buf)
;;                                  (.order ByteOrder/LITTLE_ENDIAN)
;;                                  .asIntBuffer)]
;;      (loop [i ^int (int 0)]
;;        (if (< i len)
;;          (do
;;            (.put byte-buf i (aget arr i))
;;            (recur (unchecked-inc-int i)))
;;          buf))))


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


(defn slice-dimension
  [file]
  (with-open [^java.io.BufferedReader rdr (clojure.java.io/reader file)]
    (let [first-line (clojure.string/split (.readLine rdr) #"\s+")]
      [(unchecked-inc (count (line-seq rdr))) (count first-line)])))


(defn find-slices
  ([^String folder]
   (find-slices folder #"x_0_\d+\.txt"))
  ([^String folder ^java.util.regex.Pattern pattern]
   {:pre [(instance? java.util.regex.Pattern pattern)]}
   (filterv (fn [^java.io.File f]
              (re-matches pattern  (.getName f)))
            (file-seq (clojure.java.io/file folder)))))


(defn dataset-dimension
  "Given dataset folder and dataset file pattern, return [rows cols slices]"
  [^String folder ^java.util.regex.Pattern pattern]
  (let [slices (find-slices folder pattern)]
    (when (not (empty? slices))
      (conj (slice-dimension (first slices)) (count slices)))))


(defn load-vctr
  (^RealBlockVector [file]
   (with-open [rdr (io/reader file)]
     (let [str-vals (s/split (slurp rdr) #"\s+")
           len (count str-vals)
           v ^RealBlockVector (dv len)
           it (clojure.lang.RT/iter str-vals)]
       (loop [i (long 0)]
         (if (.hasNext it)
           (do (v i (java.lang.Double/parseDouble (.next it)))
               (recur (unchecked-inc i)))
           v)))))

  (^RealBlockVector [file ^RealBlockVector vctr]
   ;; (println "load-vctr2 v0.2")
   (with-open [rdr (io/reader file)]
     (let [str-vals (s/split (slurp rdr) #"\s+")]
       (assert (= (dim vctr) (count str-vals)))
       (reduce-kv (fn [acc k v]
                    (acc k (java.lang.Float/parseFloat v))
                    acc)
                  vctr
                  str-vals)))))


;; (defn load-double-array
;;   (^doubles [^String filename]
;;    (load-double-array filename nil))
;;   (^doubles [^String filename opts]
;;    (let [offset (or (:offset opts) 0)
;;          data   (or (:data opts) ())]
;;     (with-open [rdr (io/reader filename)]
;;       (let [str-vals (s/split (slurp rdr) #"\s+")
;;             len (count str-vals)
;;             data (double-array len)
;;             it (clojure.lang.RT/iter str-vals)]
;;         (dotimes [i len]
;;           (aset data i (java.lang.Double/parseDouble (.next it))))
;;         data)))))


;; (defn load-slice
;;   [^String filename opts]
;;   (case (:type opts)
;;     :array  (load-double-array filename opts)
;;     :vector (load-vctr filename opts)
;;     (do (println "Unknow type"))))


(defn load-series
  "folder  : dataset folder
   pattern : file pattern, need to include group pattern"
  ;; e.g. #"x_0_(\d+)\.txt"
  (^RealBlockVector [folder pattern]
   (load-series folder pattern nil))

  (^RealBlockVector [folder pattern opts]
   (if-let [[^long rows ^long cols ^long slices-found] (if (and (:rows opts) (:cols opts) (:slices opts))
                                                         [(long (:rows opts)) (long (:cols opts)) (long (:slices opts))]
                                                         (dataset-dimension folder pattern))]
     (do (println "dataset found, dimension:" [rows cols slices-found])
         (println "requested slices: " (:slices opts))
         (let [^long slices (if-let [s (:slices opts)]
                              (if (<= (long s) slices-found)
                                (long s)
                                (throw (Exception. (format "Error: requested %d slices, but only %d available."
                                                           s slices-found))))
                              (long slices-found))
               sorted-files (sort-by (fn [^java.io.File f]
                                       (let [match (re-find pattern (.getName f))]
                                         (if (coll? match)
                                           (-> match
                                               second
                                               Integer/parseInt)
                                           (throw (Exception. (format "Error: Pattern %s does not contain group" pattern))))))
                                     (find-slices folder pattern))
               length (* (long rows) (long cols))
               x (dv (* (long length) (long slices)))]
           (println (format "Loading %d slices" slices))
           (let [it (clojure.lang.RT/iter sorted-files)]
             (loop [i (long 0)]
               (if (and (< i slices) (.hasNext it))
                 (let [^java.io.File f (.next it)
                       sv (subvector x (* i length) length)]
                   (println (format "%3d : %s" i (.getName f)))
                   (load-vctr f sv)
                   (recur (unchecked-inc i)))
                 x)))))
     (println "dataset not found"))))


;; (defn load-series2
;;   "Use java array, not yet finished"
;;   ^doubles [prefix & {:keys [rows cols slices ext iter] :or {rows 200 cols 200 slices 16 ext "txt" iter 0}}]
;;   ;;Verify files
;;   (dotimes [i slices]
;;     (let [fname (format "%s_%d_%d.%s" prefix iter i ext)]
;;       (if (.exists ^File (File. fname))
;;         (timbre/info (format "%s ... OK." fname ))
;;         (java.io.FileNotFoundException. (format "%s not found" fname)))))

;;   (let [length (* ^long rows ^long cols)
;;         x (double-array (* length ^long slices))]
;;     (loop [offset (long 0)
;;            i (long 0)]
;;       (if (< i ^long slices)
;;         (let [fname (format "%s_%d_%d.%s" prefix iter i ext)
;;               sv (subvector x offset length)]
;;           (load-vctr fname sv)
;;           (recur (+ offset  length) (unchecked-inc-int i)))
;;         x))))


(defn save-x-txt
  [^RealBlockVector x rows cols fname & {:keys [folder] :or {folder "."}}]
  {:pre [(= (dim x) (* (long rows) (long cols)))]}
  (let [fname (format "%s/%s.txt" folder fname)
        file ^File   (java.io.File. fname)
        z    ^double (double 0.0)]
    (assert (= (dim x) (* (long rows) (long cols))))
    (if (.exists file)
      (throw (Exception. (format "Error: File %s already exists." fname)))
      (do (when-let [d ^File (.getParentFile file)]
            (.mkdirs d))
          (let [fmt ^java.text.DecimalFormat (java.text.DecimalFormat. "0.#######")]
            (with-open [w (clojure.java.io/writer fname :append false)]
              (loop [s-vals (partition rows (map #(if (= % z) "0" (.format fmt %)) x))]
                (when-let [[line & rst] s-vals]
                  (.write w (format "%s\n" (clojure.string/join " " line)))
                  (recur rst)))))))))


;; (defn save-x2
;;   "Use java array, not yet finished."
;;   [x fname & {:keys [rows cols]}]
;;   {:pre [(int? rows) (int? cols) (= (dim x) (* (long rows) (long cols)))]}
;;   (let [file ^File (java.io.File. ^String fname)
;;         z ^float (float 0)]
;;     (when-let [d ^File (.getParentFile file)]
;;       (.mkdirs d))
;;     (let [fmt ^java.text.DecimalFormat (java.text.DecimalFormat. "0.#######")]
;;       (with-open [w (clojure.java.io/writer fname :append false)]
;;         (loop [s-vals (partition rows (map #(if (= % z) "0" (.format fmt %)) x))]
;;           (when-let [[line & rst] s-vals]
;;             (.write w (format "%s \n" (clojure.string/join " " line)))
;;             (recur rst)))))))


;; (defn save-img
;;   "Save a slice as a png image"
;;   [^doubles x [^long rows ^long cols] & {:keys [file-prefix iter slice-idx copy?]
;;                                          :or {file-prefix "x"
;;                                               iter (long 0)
;;                                               slice-idx (long 0)
;;                                               copy? true}}]
;;   {:pre [(= (alength x) (* rows cols))]}
;;   (let [image ^BufferedImage (BufferedImage. cols rows BufferedImage/TYPE_BYTE_GRAY)
;;         len ^int (alength x)
;;         data ^doubles (if copy? (Arrays/copyOf x len) x)
;;         [^double _min ^double _max] (pct.common/min-max-doubles data)
;;         _r ^double (- _max _min)]
;;     (when-not (= _r 0.0)
;;       (dotimes [i len]
;;         (aset data i (double (Math/round (* (/ (- (aget data i) _min) _r) 255))))))
;;     (.setPixels ^WritableRaster (.getRaster image) 0 0 cols rows data)
;;     (ImageIO/write image "png" (clojure.java.io/file (format "%s_%s_%s.png" file-prefix iter slice-idx)))))


(defn save-x-img
  [^RealBlockVector x rows cols fname & {:keys [folder max min] :or {folder "."}}]
  {:pre [(= (dim x) (* (long rows) (long cols)))]}
  (let [fname (format "%s/%s.png" folder fname)
        im-file ^File (java.io.File. fname)
        z    ^double (double 0.0)]
    (assert (= (dim x) (* (long rows) (long cols))))
    (if (.exists im-file)
      (let [msg (format "Error: File %s already exists." fname)]
        (println msg)
        (timbre/info msg)
        nil)
      (do (when-let [d ^File (.getParentFile im-file)]
            (.mkdirs d))
          (let [len  (* (long rows) (long cols))
                ^doubles data (transfer! x (double-array len))
                image ^BufferedImage (BufferedImage. cols rows BufferedImage/TYPE_BYTE_GRAY)
                [^double _min ^double _max] (if (and max min)
                                              [(double min) (double max)]
                                              (pct.common/min-max-doubles data))
                _r ^double (- _max _min)]
            (when-not (= _r 0.0)
              (dotimes [i len]
                (aset data i (double (Math/round (* (/ (- (aget data i) _min) _r) 255))))))
            (.setPixels ^WritableRaster (.getRaster image) 0 0 (long cols) (long rows) data)
            (ImageIO/write image "png" im-file))))))


(defn save-recon-opts [recon-opts folder & {:keys [name]}]
  (let [fname (format "%s/%s" folder (or name "recon_config.json"))]
    (with-open [f (clojure.java.io/writer fname)]
      (let [#_#_recon-opts (-> recon-opts
                           (#(if (:date %)     % (assoc % :date     (pct.util.system/timestamp))))
                           (#(if (:hostname %) % (assoc % :hostname (pct.util.system/hostname)))))]
        (.write f (generate-string recon-opts
                                   {:pretty true
                                    :key-fn #(if (clojure.core/keyword? %)
                                               (clojure.core/name %)
                                               (clojure.core/str %))}))))))


(defn load-recon-opts
  [folder & {:keys [name]}]
  (let [recon-file (java.io.File. (format "%s/%s" folder (or name "recon_config.json")))]
    (when (and (.exists recon-file) (.isFile recon-file))
      (parse-string (slurp (.getPath recon-file))
                    #(if (re-matches #"\d+" %)
                       (java.lang.Long/parseLong %)
                       (keyword %))))))


(defn export-data [data path]
  (let [^java.io.File fout   (clojure.java.io/file path)
        ^java.io.File folder (.getParentFile fout)]
    #_(assert (not (.exists fout)) (format "File exists: %s" path))
    #_(println "Data = " data)
    (if (.exists fout)
      (println (format "File exists: %s. Skip." path))
      (do (when (not (.exists folder))
            (.mkdirs folder))
          (with-open [f (clojure.java.io/writer path)]
            (.write f (generate-string (-> data
                                           (#(if (:timestamp %) % (assoc % :timestamp (pct.util.system/timestamp))))
                                           (#(if (:hostname  %) % (assoc % :hostname  (pct.util.system/hostname)))))
                                       {:pretty true
                                        :key-fn #(if (clojure.core/keyword? %)
                                                   (clojure.core/name %)
                                                   (clojure.core/str %))})))))))


(defn load-data
  [^String path]
  (let [f (java.io.File. path)]
    (when (and (.exists f) (.isFile f))
      (parse-string (slurp (.getPath f))
                    #(if (re-matches #"\d+" %)
                       (java.lang.Long/parseLong %)
                       (keyword %))))))


(defn save-series
  "Save series, as well as recon parameters if given"
  ([^RealBlockVector x  rows  cols slices]
   (save-series x rows cols slices {:type :txt}))
  ([^RealBlockVector x  rows  cols slices opts]
   (let [slice-offset (* (long rows) (long cols))
         output_type (or (:type opts) :txt)
         ^String iter (if-let [i (:iter opts)]
                        (format "%02d" i) "?")
         #_#_iter (if-let [iter (:iterations recon-opts)] (long iter) 0)
         ^String folder (or (:folder opts)
                            (let [timestamp (pct.util.system/timestamp)
                                  hostname  (pct.util.system/hostname)]
                              (format "%s_%s" hostname timestamp)))]
     (.mkdirs (java.io.File. folder))
     ;; write recon-opts
     (when-let [recon-opts (:recon-opts opts)]
       (save-recon-opts recon-opts folder))
     #_(with-open [f (clojure.java.io/writer (format "%s/%s" folder "recon_config.json"))]
         (let [recon-opts (-> recon-opts
                              (#(if (:date %)     % (assoc % :date     timestamp)))
                              (#(if (:hostname %) % (assoc % :hostname (pct.util.system/hostname)))))]
           (.write f (generate-string recon-opts
                                      {:pretty true
                                       :key-fn (fn [k] (if (keyword? k)
                                                        (name k)
                                                        (str k)))}))))
     (case output_type
       :txt (do (dotimes [i slices]
                  (save-x-txt (subvector x (* i slice-offset) slice-offset) rows cols (format "x_%s_%03d" iter i) :folder folder))
                true)
       :img (do (dotimes [i slices]
                  (save-x-img (subvector x (* i slice-offset) slice-offset) rows cols (format "x_%s_%03d" iter i) :folder folder))
                true)
       :all (do (dotimes [i slices]
                  (let [name (format "x_%s_%03d" iter i)]
                    (save-x-txt (subvector x (* i slice-offset) slice-offset) rows cols name :folder folder)
                    (save-x-img (subvector x (* i slice-offset) slice-offset) rows cols name :folder folder)))
                true)
       (let [msg (format "Unknown type: %s. Nothing to do here." (str output_type))]
         (println msg)
         (timbre/info msg))))))


(defn save-result
  [^HashMap results rows cols slices recon-opts opts]
  {:pre [#_(and (:x0 opts) (:regions opts) (:samples opts))]}
  (let [^String folder (let [timestamp (or (-> results :properties :timestamp) (pct.util.system/timestamp))
                             hostname  (or (-> results :properties :hostname)  (pct.util.system/hostname))]
                         (format "%s/%s_%s" (or (:folder opts) ".") hostname timestamp))
        folder_f (java.io.File. folder)]
    (if (.exists folder_f)
      (println (format "Folder %s already exists, results probably already saved. skip." folder))
      (do (.mkdirs folder_f)
          (save-recon-opts (-> recon-opts
                               (assoc :properties (.get results :properties))
                               (assoc :result (:result opts)))
                           folder)
          (doseq [[k v] results]
            (when (int? k)
              (save-series (first v) rows cols slices
                           (-> opts
                               (assoc  :iter k)
                               (assoc  :folder folder)
                               (dissoc :recon-opts)))))))))


(comment
  ;; read config using clojure.data
  (json/read-str (slurp "test_confi.txt") :key-fn #(if (re-matches #"\d+" %)
                                                     (java.lang.Long/parseLong %)
                                                     (keyword %)))

  ;; read config using cheshire.core
  (parse-string (slurp "test_config.txt") (fn [k] (if (re-matches #"\d+" k)
                                                   (java.lang.Long/parseLong k)
                                                   (keyword k))))
  ;; write config using cheshire.core
  (with-open [f (clojure.java.io/writer "test_config.txt")]
    (.write f (generate-string {:tvs? true
                                :iterations 6
                                :lambda {1 0.00001
                                         2 0.00001
                                         3 0.00001
                                         4 0.00001
                                         5 0.00001}}
                       {:pretty true
                        :key-fn (fn [k] (if (keyword? k)
                                         (clojure.core/name k)
                                         (str k)))}))))

;; (defn save-series [x prefix & {:keys [rows cols ext binary filename]
;;                                :or   {rows 200 cols 200 ext "txt" binary false}}]
;;   (let [length (* ^long rows ^long cols)
;;         len ^long (dim x)]
;;     (if binary
;;       ;; Write image as a binary file
;;       ;; start with rows, cols, slices stored as integer, machine format
;;       ;; followed by x, stored as double array machine format
;;       (let [slices (/ len length)
;;             fname (format "%s/%s" prefix (or filename "x.bin"))
;;             file ^File (java.io.File. fname)]
;;         (when-let [d ^File (.getParentFile file)]
;;           (.mkdirs d))
;;         (with-open [os (clojure.java.io/output-stream fname)]
;;           (let [rows-buff   (ByteBuffer/allocate 4)
;;                 cols-buff   (ByteBuffer/allocate 4)
;;                 slices-buff (ByteBuffer/allocate 4)
;;                 dbuff       (ByteBuffer/allocate (* len 8))]
;;             (-> (.order rows-buff ByteOrder/LITTLE_ENDIAN)
;;                 .asIntBuffer
;;                 (.put ^int rows))
;;             (-> (.order cols-buff ByteOrder/LITTLE_ENDIAN)
;;                 .asIntBuffer
;;                 (.put ^int cols))
;;             (-> (.order slices-buff ByteOrder/LITTLE_ENDIAN)
;;                 .asIntBuffer
;;                 (.put ^int slices))
;;             (.write os (.array rows-buff))
;;             (.write os (.array cols-buff))
;;             (.write os (.array slices-buff))
;;             (let [buf ^DoubleBuffer (-> (.order dbuff ByteOrder/LITTLE_ENDIAN)
;;                                         .asDoubleBuffer)]
;;               (loop [i (long 0)]
;;                 (if (< i len)
;;                   (do (.put buf i (x i))
;;                       (recur (inc i)))
;;                   (.write os (.array dbuff))))))))
;;       ;; Write each slice as text image
;;       (loop [offset (int 0)
;;              i (int 0)]
;;         (when (< offset len)
;;           (save-x (subvector x offset length) prefix :id i :rows rows :cols cols :ext ext)
;;           (recur (unchecked-add-int offset length) (unchecked-inc-int i)))))))



(defn createResultFolder ^java.lang.String []
  (loop [folder ^java.io.File (java.io.File. (format "results/%s" (pct.util.system/timestamp)))]
    (if (.exists folder)
      (recur (java.io.File. (format "results/%s" (pct.util.system/timestamp))))
      (do (.mkdirs folder)
          (.toString folder)))))


;; (defn save-series2 [x config]
;;   (let [folder (createResultFolder)
;;         iter (or (:iterations config) 0)
;;         {:keys [rows cols slices]} config]
;;     (assert (or (nil? rows) (nil? cols) (nil? slices)))
;;     (with-open [f (clojure.java.io/writer (format "%s/config.txt" folder))]
;;       (.write f (generate-string config {:pretty true
;;                                          :key-fn (fn [k] (if (keyword? k) (clojure.core/name k) (str k)))}))
;;       )))


(defn load-data-sample
  [f]
  (with-open [scanner ^Scanner (Scanner. (clojure.java.io/as-file f))]
    (let [id ^long (java.lang.Long/parseLong (.nextLine scanner))
          acc ^ArrayList (ArrayList.)]
      (loop []
        (when (.hasNext scanner)
          (.add acc (java.lang.Long/parseLong (.next scanner)))
          (recur)))
      ;; convert to array
      (let [n   ^long (long (.size acc))
            arr ^ints (int-array (.size acc))]
        (loop [i (long 0)]
          (if (< i n)
            (do (aset arr i (int (.get acc i)))
                (recur (unchecked-inc i)))
            [id arr]))))))


#_(defn dumpToMatlab [file x history lambda]
  (let [f (if (instance? java.io.File file ))]))


(defn ProtonHistory->fromStream
  "Read the path and b data from two seperate files provided by Paniz, each one contains everything.
   MLP-stream : BufferedInputStream for MLP path file
   WEPL-stream : BufferedInputstream for WEPL file, if provided; otherwise, WEPL data is provided by MLP path file

   Return a ProtonHistory"

  (^ProtonHistory [^long id ^java.io.BufferedInputStream MLP-stream]
   (when-let [^int length (read-int MLP-stream __ENDIAN__ __int_buffer__)]
     (when (> length __max_intersections__)
       (throw (Exception. (format "Error: length (%d) > __max_intersections__ (%d)." length __max_intersections__))))
     (let [ ;; path-buf ^bytes (byte-array (* 4 length))
           path-arr ^ints  (int-array length)]
       (if (= (.read MLP-stream __path_buffer__ 0 (* 4 length)) -1)
         (throw (java.io.IOException. "Error: Unexpected EOF when trying to read MLP data (int array)."))
         (do (-> (ByteBuffer/wrap ^bytes __path_buffer__)
                 (.order __ENDIAN__)
                 .asIntBuffer
                 (.get path-arr 0 length))
             (let [^double chord-len (read-double MLP-stream __ENDIAN__ __double_buffer__)
                   ^float  wepl      (read-float  MLP-stream __ENDIAN__ __float_buffer__)
                   ^float  entry-xy  (read-float  MLP-stream __ENDIAN__ __float_buffer__ )
                   ^float  entry-xz  (read-float  MLP-stream __ENDIAN__ __float_buffer__ )
                   ^float  exit-xy   (read-float  MLP-stream __ENDIAN__ __float_buffer__ )
                   ^float  exit-xz   (read-float  MLP-stream __ENDIAN__ __float_buffer__)]
               (ProtonHistory. id path-arr chord-len wepl entry-xy entry-xz exit-xy exit-xz
                               (new HashMap))))))))

  (^ProtonHistory [^long id ^java.io.BufferedInputStream MLP-stream ^java.io.BufferedInputStream WEPL-stream]
   (when-let [^int length (read-int MLP-stream __ENDIAN__ __int_buffer__)]
     (when (> length __max_intersections__)
       (throw (Exception. (format "Error: length (%d) > __max_intersections__ (%d)." length __max_intersections__))))
     (let [ ;; path-buf ^bytes (byte-array (* 4 length))
           path-arr ^ints  (int-array length)]
       (if (= (.read MLP-stream __path_buffer__ 0 (* 4 length)) -1)
         (throw (java.io.IOException. "Error: Unexpected EOF when trying to read MLP data (int array)."))
         (do (-> (ByteBuffer/wrap ^bytes __path_buffer__)
                 (.order __ENDIAN__)
                 .asIntBuffer
                 (.get path-arr 0 length))
             (let [^double chord-len (read-double MLP-stream __ENDIAN__ __double_buffer__)
                   ^float  entry-xy  (read-float  MLP-stream __ENDIAN__ __float_buffer__ )
                   ^float  entry-xz  (read-float  MLP-stream __ENDIAN__ __float_buffer__ )
                   ^float  exit-xy   (read-float  MLP-stream __ENDIAN__ __float_buffer__ )
                   ^float  exit-xz   (read-float  MLP-stream __ENDIAN__ __float_buffer__ )
                   ^float  wepl      (read-float WEPL-stream __ENDIAN__ __float_buffer__)]
               (ProtonHistory. id path-arr chord-len wepl entry-xy entry-xz exit-xy exit-xz
                               (new HashMap)))))))))


(defprotocol IPCTDataset
  (size   [this]  "return a vector of [rows cols slices]")
  (files  [this] "get data file paths")
  (slice-offset [this] "get length of each slice")

  ;; ;; from IHistoryInput
  ;; (skip* [this n]   "skip n data")
  ;; (next* [this] [this n] [this n out-ch]
  ;;   "read next data, if n is given will read the next n data to acc.
  ;;    If acc could be either a container or a channel")
  (rest* [this] [this out-ch] [this out-ch batch] "read the remaining data to out channel")
  (reset* [this]    "reset position")
  (read-ProtonHistory [this] [this out-ch] [this out-ch batch] "read data from file as ProtonHistory directly")
  (length* [this] )
  (count-test [this] [this m]))


(deftype PCTDataset [^long rows ^long cols ^long slices ^long history-count
                     ^java.lang.String MLP-file ^java.lang.String WEPL-file
                     ^{:unsynchronized-mutable true :tag java.io.BufferedInputStream} MLP-stream
                     ^{:unsynchronized-mutable true :tag java.io.BufferedInputStream} WEPL-stream
                     ^{:unsynchronized-mutable true :tag long} index
                     ^HashMap samples
                     ^RealBlockVector x0]
  IPCTDataset
  ;; (skip* [this k]
  ;;   (let [last-idx (+ index ^long k)]
  ;;     (if WEPL-stream
  ;;       (loop [i index]
  ;;         (if (< i last-idx)
  ;;           (when-let [b (HistoryBuffer->fromStream i MLP-stream WEPL-stream)]
  ;;             (recur (unchecked-inc i)))
  ;;           (set! index i)))
  ;;       (loop [i index]
  ;;         (if (< i last-idx)
  ;;           (when-let [b (HistoryBuffer->fromStream i MLP-stream WEPL-stream)]
  ;;             (recur (unchecked-inc i)))
  ;;           (set! index i))))
  ;;     this))

  ;; (next* [this]
  ;;   (when open?
  ;;     (let [s (HistoryBuffer->fromStream index MLP-stream WEPL-stream)]
  ;;       (set! index (inc index))
  ;;       s)))

  ;; (next* [this n]
  ;;   (let [acc ^ArrayList (ArrayList. (int n))
  ;;         last-idx (+ index ^long n)]
  ;;     (loop [i index]
  ;;       (if (< i last-idx)
  ;;         (when-let [b (HistoryBuffer->fromStream i MLP-stream WEPL-stream)]
  ;;           (.add acc b)
  ;;           (recur (inc i)))
  ;;         (set! index i)))
  ;;     acc))

  ;; (next* [this n out-ch]
  ;;   (let [last-idx (+ index ^long n)]
  ;;     (loop [i index]
  ;;       (if (< i last-idx)
  ;;         (when-let [b (HistoryBuffer->fromStream i MLP-stream WEPL-stream)]
  ;;           (>!! out-ch b)
  ;;           (recur (inc i)))
  ;;         (set! index i))))
  ;;   (a/close! out-ch))

  (rest* [this out-ch] ;; read
    (loop []
      (when-let [b (if WEPL-stream
                     (ProtonHistory->fromStream index MLP-stream WEPL-stream)
                     (ProtonHistory->fromStream index MLP-stream))]
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
        (if-let [b (if WEPL-stream
                     (ProtonHistory->fromStream c MLP-stream WEPL-stream)
                     (ProtonHistory->fromStream c MLP-stream))]
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

  (read-ProtonHistory [this]
    (if-let [^ProtonHistory s (if WEPL-stream
                                (ProtonHistory->fromStream index MLP-stream WEPL-stream)
                                (ProtonHistory->fromStream index MLP-stream))]
      (do (set! index (inc index))
          s)))

  (read-ProtonHistory [this out-ch]
    (loop []
      (when-let [^ProtonHistory b (if WEPL-stream
                                    (ProtonHistory->fromStream index MLP-stream WEPL-stream)
                                    (ProtonHistory->fromStream index MLP-stream))]
        (set! index (inc index))
        (>!! out-ch b)
        (recur)))
    (a/close! out-ch))

  (read-ProtonHistory [this out-ch batch-size]
    ;; read in data as ProtonHistory
    (let [batch-size ^long batch-size]
      (loop [acc ^objects (object-array batch-size)
             len ^long    (long 0)
             id  ^long    (long 0)]
        (if (< id history-count)
          (if-let [^ProtonHistory b (if WEPL-stream
                                      (ProtonHistory->fromStream id MLP-stream WEPL-stream)
                                      (ProtonHistory->fromStream id MLP-stream))]
            (if (< len batch-size)
              (do (aset acc len b)
                  (recur acc (unchecked-inc len) (unchecked-inc id)))
              (let [new-acc ^objects (object-array batch-size)]
                (>!! out-ch [len acc])
                (aset new-acc 0 b)
                (recur new-acc (long 1) (unchecked-inc id))))
            (do (>!! out-ch [len acc])))
          (do (timbre/info (format "read-ProtonHistory: Expected number of histories (%d) are read." id))
              (.close this)
              (a/close! out-ch))))))

  (count-test [this]
    (loop [i ^long (long 0)]
      (if-let [b (if WEPL-stream
                   (ProtonHistory->fromStream i MLP-stream WEPL-stream)
                   (ProtonHistory->fromStream i MLP-stream))]
        (recur (unchecked-inc i))
        i)))

  (count-test [this m]
    (let [n (.size ^HashMap m)
          res ^HashMap (HashMap.)]
      (loop [i          ^long (long 0)
             test-count ^long (long 0)]
        (if (< test-count n)
          (if-let [^ProtonHistory b (if WEPL-stream
                                      (ProtonHistory->fromStream i MLP-stream WEPL-stream)
                                      (ProtonHistory->fromStream i MLP-stream))]
            (do (if-let [arr ^ints (.get ^HashMap m i)]
                  (do (if (Arrays/equals arr ^ints (.path b))
                        (.put res i true)
                        (.put res i {:path (.path b) :sample arr}))
                      (recur (unchecked-inc i) (unchecked-inc test-count)))
                  (recur (unchecked-inc i) test-count))))
          res))))

  (length* [_] history-count)


  ;; clojure.lang.Seqable
  ;; (seq [this]
  ;;   (sseq this next*))

  ;; -------------------------------

  (size  [_]  [rows cols slices])
  (files [_] {:MLP-file  MLP-file
              :WEPL-file WEPL-file})

  (slice-offset [_] (* rows cols))

  clojure.lang.Counted
  (count [_] history-count)

  pct.data.ICloseable
  (close* [this]
    (.close this))

  java.lang.AutoCloseable
  (close [_]
    (timbre/info "closing streams in PCTDataset")
    (.close MLP-stream)
    (when WEPL-file
      (.close WEPL-stream))))


(defn test-dataset [^PCTDataset dataset]
  (count-test dataset (.samples dataset)))


(defn newPCTDataset
  ([^String folder ^String MLP-file]
   (newPCTDataset folder MLP-file nil))
  ([^String folder ^String MLP-file ^String WEPL-file]
   (let [[^long rows ^long cols ^long slices] (dataset-dimension folder #"x_0_\d+\.txt")]
     (let [MLP-stream (BufferedInputStream. (FileInputStream. (format "%s/%s" folder MLP-file)) __IO_buffer_size__)
           ;; MLP-stream (clojure.java.io/input-stream (format "%s/%s" folder MLP-file))
           history-count (read-header MLP-stream)
           _ (println "history-count = " history-count)
           WEPL-stream (if WEPL-file
                         (clojure.java.io/input-stream (format "%s/%s" folder WEPL-file))
                         ;; (BufferedInputStream. (FileInputStream. (format "%s/%s" folder WEPL-file)) __IO_buffer_size__)
                         nil)]
       (when WEPL-stream
         (assert (= (read-header WEPL-stream) history-count)))
       (let [samples ^HashMap (HashMap.)]
         (loop [i (long 0)]
           (let [f ^java.io.File (clojure.java.io/file (format "%s/path_%d.txt" folder i))]
             (when (.isFile f)
               (let [[id data] (load-data-sample f)]
                 (.put samples id data)
                 (recur (unchecked-inc i))))))
         (->PCTDataset rows cols slices history-count
                       MLP-file WEPL-file MLP-stream WEPL-stream
                       (long 0)
                       samples
                       (load-series folder #"x_0_(\d+)\.txt" {:rows rows :cols cols :slices slices})))))))


#_(defn newPCTDataset [{:keys [rows :rows cols :cols slices :slices dir :dir path :path b :b fmt :fmt] :as input}]
  (let [MLP-file (format "%s/%s" dir path)
        WEPL-file (when (= fmt :old) (format "%s/%s" dir b))]
    ;; (println input)
    #_(map->PCTDataset {:rows rows :cols cols :slices slices
                        :MLP-file MLP-file
                        :WEPL-file WEPL-file
                        :in-stream (pct.data/newHistoryInputStream MLP-file WEPL-file)
                        :x0 (pct.data.io/load-series (format "%s/x" dir)
                                                     :rows rows :cols cols :slices slices :ext "txt" :iter 0)})
    (try
      (let [samples ^HashMap (HashMap.)]
        (loop [i (long 0)]
          (let [f ^java.io.File (clojure.java.io/file (format "%s/path_%d.txt" dir i))]
            (when (.isFile f)
              (let [[id data] (load-data-sample f)]
                (.put samples id data)
                (recur (unchecked-inc i))))))
        (->PCTDataset rows cols slices MLP-file WEPL-file
                      samples
                      (load-series dir #"x_0_(\d+)\.txt")
                      (if WEPL-file
                        (pct.data/newHistoryInputStream MLP-file WEPL-file)
                        (pct.data/newHistoryInputStream MLP-file))
                      fmt))
      (catch java.io.FileNotFoundException ex
        (timbre/error ex "File is not found." (.getName (Thread/currentThread)))
        (throw ex)))))


(defn load-dataset
  ^pct.data.HistoryIndex [^PCTDataset dataset opts]
  {:pre []
   :post [(= (reduce + (spr/transform spr/ALL (fn [[[data _] & _]] (count data)) (seq %)))
             (count %))
          (if (:count? opts)
            (= (pct.data/total-voxel-hits % :hitmap) (pct.data/total-voxel-hits % :pathdata))
            (= (pct.data/total-voxel-hits % :hitmap) 0))]}
  (let [jobs       (long (or (:jobs opts) (- ^int pct.util.system/PhysicalCores 4)))
        min-len    (long (or (:min-len opts) 0))
        batch-size (long (or (:batch-size opts) 20000))
        count?     (boolean (or (:count? opts) false))
        global?    (if (:global? opts) true false)
        in-ch      (a/chan jobs)
        init-x     ^RealBlockVector (.x0 dataset)
        offset     ^long (slice-offset dataset)
        [rows cols slices] (size dataset)
        ]
    (timbre/info (format "Loading dataset 2 with %d workers, batch size = %d" jobs batch-size))
    (let [res-ch (do (timbre/info "Using new style")
                     (pct.async.threads/asyncWorkers
                      jobs
                      (fn [[^long len ^objects batch]]
                        (let [acc (new HashMap)]
                          (loop [i (long 0)]
                            (if (< i len)
                              (let [data ^pct.data.ProtonHistory (aget batch i)]
                                (when (>= (count data) min-len)
                                  (let [[^int b ^int e] (pct.common/min-max-ints ^ints (.path data))
                                        start-idx ^long (long (quot b ^long offset))
                                        end-idx   ^long (long (quot e ^long offset))
                                        len (unchecked-inc (- end-idx start-idx))]
                                    (when (not global?)
                                      (pct.common/trim-ints (.path data) (* start-idx ^long offset) true))
                                    (doto ^HashMap (.properties data)
                                      (.put :first-slice start-idx)
                                      (.put :last-slice  end-idx))
                                    (if-let [acc-idx ^HashMap (.get acc start-idx)]
                                      (if-let [acc-idx-len ^ArrayList (.get acc-idx len)]
                                        (.add acc-idx-len data)
                                        (let [a  (new ArrayList)]
                                          (.add a data)
                                          (.put acc-idx len a)))
                                      (let [acc-idx (new HashMap)
                                            a       (new ArrayList)]
                                        (.add a data)
                                        (.put acc-idx len a)
                                        (.put acc start-idx acc-idx)))))
                                (recur (unchecked-inc i)))))
                          acc))
                      (fn
                        ([] (pct.data/newHistoryIndex rows cols slices :global? global? :x0 init-x))
                        ([acc] acc)
                        ([^pct.data.HistoryIndex acc ^HashMap m]
                         (pct.data/mergeIndex* acc m)
                         (.clear m)
                         acc))
                      in-ch))
          res (try (pct.common/with-out-str-data-map
                     (time (do (timbre/info "Start indexing ....")
                               #_(pct.data/read-ProtonHistory (.in-stream dataset) in-ch batch-size)
                               (read-ProtonHistory dataset in-ch batch-size)
                               (let [index (a/<!! res-ch)]
                                 (timbre/info "Finished making index." )
                                 index))))
                   (catch Exception ex
                     (a/close! in-ch)
                     (timbre/error ex "[load-dataset] Something went wrong in feeder" (.getName (Thread/currentThread)))))]
      (timbre/info (clojure.string/replace (:str res) #"[\n\"]" ""))
      (if count?
        (let [res (pct.common/with-out-str-data-map
                    (time
                     (pct.data/count-voxel-hits  (:ans res) {:forced true :jobs jobs :batch-size 100000})))]
          (timbre/info (clojure.string/replace (:str res) #"[\n\"]" ""))
          (:ans res))
        (:ans res)))))


(defn count-dataset-test [^PCTDataset dataset opts]
  (let [jobs       (long (or (:jobs opts) (- ^int pct.util.system/PhysicalCores 4)))
        batch-size (long (or (:batch-size opts) 20000))
        in-ch      (a/chan jobs)
        threads    (boolean (or (:threads opts) false))
        init-x     ^RealBlockVector (.x0 dataset)
        offset     (slice-offset dataset)
        [rows cols slices] (size dataset)]

    (if threads
      (do (timbre/info (format "Loading dataset with %d workers, batch size = %d" jobs batch-size))
          (let [res-ch (pct.async.threads/asyncWorkers
                        jobs
                        (fn [[^long len ^objects data-arr]]
                          ;; processing bulk HistoryBuffer into ProtonHistory
                          len)
                        (fn
                          ([] (long 0))
                          ([acc] acc)
                          ([^long acc ^long v]
                           (unchecked-add acc v)))
                        in-ch)
                res (try (pct.common/with-out-str-data-map
                           (time (do (timbre/info "Start indexing ....")
                                     (rest* dataset in-ch batch-size)
                                     (let [index (a/<!! res-ch)]
                                       (timbre/info "Finished making index." )
                                       index))))
                         (catch Exception ex
                           (a/close! in-ch)
                           (timbre/error ex "[load-dataset, multi threads] Something went wrong in feeder"
                                         (.getName (Thread/currentThread)))))]
            (timbre/info (clojure.string/replace (:str res) #"[\n\"]" ""))
            (:ans res)))
        (try (let [res (pct.common/with-out-str-data-map
                         (time (count-test dataset)))]
               (timbre/info (clojure.string/replace (:str res) #"[\n\"]" ""))
               (:ans res))
             (catch Exception ex
               (timbre/error ex "[load-dataset, single threads] Something went wrong in count-test."
                             (.getName (Thread/currentThread))))))))

(defn verify-dataset
  "Verify consistency of dataset based on given random samples"
  [^PCTDataset dataset opts]
  )

