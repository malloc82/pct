(ns sandbox.io
  (:use clojure.core)
  (:import [java.nio ByteOrder ByteBuffer IntBuffer]
           [java.io FileInputStream BufferedOutputStream BufferedInputStream RandomAccessFile File InputStreamReader BufferedReader]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(def ^{:private true :tag 'bytes} __int_buffer__    (byte-array 4))
(def ^{:private true :tag 'bytes} __long_buffer__   (byte-array 8))
(def ^{:private true :tag 'bytes} __float_buffer__  (byte-array 4))
(def ^{:private true :tag 'bytes} __double_buffer__ (byte-array 8))
(def ^{:private true :tag 'long}  __header_length__ 32)
(def ^{:private true :tag 'int}   __max_intersections__ (int 1280)) ;; according to pCT_reconstruction code base
(def ^{:private true :tag 'ints}  __path_buffer__   (byte-array (* 4 __max_intersections__))) ;; 1280 is the max intersections
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
   If a byte buffer is provided, use thnnnnnnne given buffer, otherwise use a local created buffer"
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


;; (defn read-header
;;   "Header format: number of histories is stored as string, followed with a newline character (0x0a)"
;;   [^BufferedInputStream is]
;;   (let [reader (BufferedReader. (InputStreamReader. is))
;;         header (.readLine reader)]
;;     (println header " " (Integer/parseInt header))))


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
         (throw (java.io.IOException. (format "Error: Unexpected end when reading header. Header buffer might be too small (%d)."
                                              buf-size))))))))

(defn- read-test
  [filename]
  (with-open [is (clojure.java.io/input-stream filename)]
    (let [endian ByteOrder/LITTLE_ENDIAN
          buffer (byte-array (* 4 100))]
      (println (read-header is))
      (let [length (read-int  is endian __int_buffer__)]
        (println "length = " length)
        (let [n 4
              ^ints arr (read-ints is endian n buffer)]
          (dotimes [i n]
            (println i ":" (aget arr i)))
          (println "\nbreak\n"))

        (let [ n 10
              ^ints arr (read-ints is endian n buffer)]
          (dotimes [i n]
            (println i ":" (aget arr i)))
          (println "\nbreak\n"))

        (let [ n 6
              ^ints arr (read-ints is endian n buffer)]
          (dotimes [i n]
            (println i ":" (aget arr i)))
          (println "\nbreak\n"))
        (let [a (read-int    is endian __int_buffer__)
              b (read-double is endian __double_buffer__)
              c (read-float  is endian __float_buffer__)
              d (read-float  is endian __float_buffer__)
              e (read-float  is endian __float_buffer__)
              ]
          (println (format "0x%08x" a) b c d e))))))

