(ns pct.data.vectors
  (:use clojure.core)
  (:require [clojure.pprint :refer [cl-format]])
  (:import [java.util Arrays]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare double-vector double-matrix)

(def ^:private compile-format #'clojure.pprint/compile-format)

(def ^:const format-g (compile-format "~6,4,,1G"))
(def ^:const format-f (compile-format "~7,4F"))
(def ^:const format-a (compile-format "~4@A~8T"))
(def ^:const format-a7 (compile-format "~4@A~7T"))
(def ^:const format-a8 (compile-format "~4@A~8T"))
(def ^:const format-seq (compile-format "~{~8A~}"))
(def ^:const format-header-row (compile-format "~{~7A~}~%"))
(def ^:const format-header-col (compile-format "~{~8A~}~%"))

(def ^:private hdots (cl-format nil format-a7 \u22ef))

(def ^{:private true :tag 'long} matrix-width  15)
(def ^{:private true :tag 'long} matrix-height 15)

(defn format-row [^java.io.Writer w s-left s-right]
  (when (seq s-left) (cl-format w format-seq s-left))
  (when (and (seq s-left) (seq s-right)) (cl-format w hdots))
  (when (seq s-right)) (cl-format w format-seq s-right))

(defprotocol IVector
  (dim [this])
  (subvector [this k l])
  (as-matrix [this r c])
  (copy [this] [this x])
  (copy-from [this arr] "Directly update with a given buffer")
  (buffer [this])
  (as-array [this])
  (master? [this])
  (v-max [this])
  (v-min [this])
  (v-min-max [this]))

(defprotocol IMatrix
  (submatrix [this r rlen c clen])
  (rows [this] "return a lazy sequence of row vectors of the matrix")
  (cols [this] "return a lazy sequence of col vectors of the matrix")
  (row [this r] "return a row vector")
  (col [this c] "return a colum vector")
  (as-vector [this]))

(defn ^:private vector-seq [vector ^long i]
  (lazy-seq
   (if (< -1 i (dim vector))
     (cons (vector i) (vector-seq vector (inc i)))
     '())))

(deftype DoubleVector [^doubles buf ^long offset ^long n ^boolean master]
  ;; stride is used for column vectors, otherwise it should always be 1
  java.lang.Object
  (equals [this y]
    (cond
      (nil? y) false
      (identical? this y) true
      (and (instance? DoubleVector y)
           (= n (dim y))
           ;; (= master (.master ^DoubleVector y))
           ;; (= offset (.offset ^DoubleVector y))
           )
      (let [len (+ offset n)]
        (loop [i offset]
          (if (< i n)
            (if (= (aget buf i) (y i))
              (recur (inc i))
              false)
            true)))
      :default false))
  (toString [_]
    (format "#DoubleVector[n:%d, offset: %d, master?: %b]"  n offset master))

  clojure.lang.Counted
  (count [_] n)

  clojure.lang.IFn
  (invoke [this]     (seq this))
  (invoke [this i]   (aget buf (+ offset ^long i)))
  (invoke [this i v] (aset buf (+ offset ^long i) ^double v))

  clojure.lang.Seqable
  (seq [this]
    (vector-seq this 0))

  IVector
  (dim ^long [this] n)

  (subvector [this k l]
    (double-vector buf k l false))

  (copy [this]
    (let [arr (Arrays/copyOfRange buf offset (+ offset n))]
      (double-vector arr 0 1 n true)))

  (copy [this x]
    (when (= n (dim x))
      (let [dst ^doubles (buffer x)]
        (System/arraycopy buf offset dst (.offset ^DoubleVector x) n)
        x)))

  (buffer [this] buf)
  (as-array [this]
    (Arrays/copyOfRange buf offset (+ offset n)))

  (master? [_] master)
  (v-max [_]
    (let [len (+ offset n)]
     (loop [_max ^double (aget buf offset)
            i (long (inc offset))]
       (if (< i len)
         (let [v (aget buf i)]
           (if (< _max v)
             (recur v    (unchecked-inc i))
             (recur _max (unchecked-inc i))))
         _max))))

  (v-min [_]
    (let [len (+ offset n)]
      (loop [_min ^double (aget buf offset)
             i (long (inc offset))]
        (if (< i len)
          (let [v (aget buf i)]
            (if (< v _min)
              (recur v    (unchecked-inc i))
              (recur _min (unchecked-inc i))))
          _min))))

  (v-min-max [_]
    (let [len (+ offset n)]
      (loop [_min ^double (aget buf offset)
             _max ^double (aget buf offset)
             i (long (inc offset))]
        (if (< i len)
          (let [v (aget buf i)]
            (if (< v _min)
              (recur v _max (unchecked-inc i))
              (if (< _max v)
                (recur _min v (unchecked-inc i))
                (recur _min _max (unchecked-inc i)))))
          [_min _max])))))

(defn double-vector
  (^DoubleVector [^long n]
   {:pre [(<= 0 n)]}
   (->DoubleVector (double-array n) 0 n true))
  (^DoubleVector [^long n ^clojure.lang.IFn$LD f]
   {:pre [(<= 0 n) (instance? clojure.lang.IFn$LD f)]}
   (let [buf ^doubles (double-array n)]
     (dotimes [i n]
       (aset buf i ^double (f i)))
     (->DoubleVector buf 0 n true)))
  (^DoubleVector [^doubles arr offset n master]
   {:pre [(<= 0 ^long offset) (<= 0 ^long n)]
    :post []}
   (->DoubleVector arr offset n master)))


(defn print-vector
    ([^java.io.Writer w formatter x]
     (when (< 0 ^long (dim x))
       (let [print-width (min ^long (dim x) matrix-width #_(long (:matrix-width @settings)))
             print-left (long (if (= print-width (dim x)) print-width (Math/ceil (/ print-width 2))))
             print-right (- print-width print-left)]
         (format-row w (map formatter (seq (subvector x 0 print-left)))
                     (map formatter (seq (subvector x (- ^long (dim x) print-right) print-right)))))))
    ([^java.io.Writer w x]
     (when (< 0 ^double (dim x))
       (.write w "\n[")
       (let [[^double min-val ^double max-val] (v-min-max x)
             formatter (partial cl-format nil
                                (if (and (not (< 0.0 min-val 0.01)) (< max-val 10000.0))
                                  format-f
                                  format-g))]

         (print-vector w formatter x))
       (.write w "]\n"))))

(defmethod print-method DoubleVector [^DoubleVector v ^java.io.Writer w]
  (.write w (str v))
  (print-vector w v))


(deftype DoubleMatrix [^doubles buf ^long offset ^long rows ^long cols ^long stride ^boolean master?]
  IVector
  (dim [this])
  (subvector [this k l])
  (as-matrix [this r c])
  (copy [this])
  (copy [this x])
  (buffer [this])
  (as-array [this])
  (master? [this])
  (v-max [this])
  (v-min [this])
  (v-min-max [this])

  IMatrix
  (submatrix [this r rlen c clen])
  (rows [this])
  (cols [this])
  (row [this r])
  (col [this c])
  (as-vector [this]))


(defn double-matrix
  (^DoubleMatrix [^long rows ^long cols]
   (->DoubleMatrix (double-array (* rows cols)) 0 rows cols cols true))
  (^DoubleMatrix [^long rows ^long cols ^clojure.lang.IFn$LLD f]
   {:pre [(<= 0 rows) (<= 0 cols) (instance? clojure.lang.IFn$LLD f)]
    :post []}
   (let [buf ^doubles (double-array (* rows cols))]
     (dotimes [r rows]
       (dotimes [c cols]
         (let [i ^long (+ (* r cols) cols)]
           (aset buf i ^double (f r c)))))
     (->DoubleMatrix buf 0 rows cols cols true )))
  (^DoubleMatrix [^doubles arr offset rows cols stride master]
   {:pre [(pos? ^long rows) (pos? ^long cols) (>= ^long stride ^long cols)]
    :post []}
   (->DoubleMatrix arr offset rows cols stride master)))

