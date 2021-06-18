(ns pct.util.prime
  (:use clojure.core)
  (:import [java.util ArrayList TreeSet]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defonce ^{:tag 'double} gr (/ 2.0 (+ (Math/sqrt 5) 1)))

(defn- prime-seq2
  ^ArrayList [^long n] ;; looking for prime number within [2, n]
  (let [mark   (byte-array n)
        sqrt_n  (int (Math/sqrt n))]
    (loop [i (int 2)]
      (if (< i sqrt_n)
        (do
          (loop [idx (* i i)]
            (when (< idx n)
              (aset mark idx (byte 1))
              (recur (+ idx i ))))
          (let [next_i (loop [i (inc i)]
                         (if (= (aget mark i) (byte 0))
                           i
                           (recur (inc i))))]
            (recur (long next_i))))
        (let [p (ArrayList.)]
          (loop [i (int 2)]
            (if (< i n)
              (do
                (when (= (aget mark i) 0)
                  (.add p i))
                (recur (inc i)))
              p)))))))

(defn- prime-seq3
  ^ints [^long n] ;; looking for prime number within [2, n]
  (let [len    (int (+ (/ n 2.0) 0.5))
        mark   (byte-array len)
        sqrt_n (int (Math/sqrt n))]
    (loop [i (int 3)]
      (if (< i sqrt_n)
        (let [ix2 (bit-shift-left i 1)]
          (loop [idx (* i i)]
            (when (< idx n)
              (aset mark (bit-shift-right idx 1) (byte 1))
              (recur (+ idx ix2))))
          (let [next_i (loop [i (bit-shift-right (+ i 2) 1)]
                         (if (= (aget mark i) (byte 0))
                           (+ (bit-shift-left i 1) 1)
                           (recur (inc i))))]
            (recur (long next_i))))
        (let [p (ArrayList.)]
          (.add p 2)
          (loop [i (int 1)]
            (if (< i len)
              (do
                (when (= (aget mark i) 0)
                  (.add p (+ (bit-shift-left i 1) 1)))
                (recur (+ i 1)))
              ;; p ;; previously
              (int-array p))))))))


(defn- prime-seq4
  ^ArrayList [^long n] ;; looking for prime number within [2, n]
  (let [len    (int (+ (/ n 2.0) 0.5))
        mark   (byte-array len)
        sqrt_n (int (/ (Math/sqrt n) 2))]
    (loop [i (int 1)]
      (if (< i sqrt_n)
        (do
          (when (= (aget mark i) (byte 0))
            (let [w (inc (bit-shift-left i 1))]
              (loop [idx (bit-shift-left (+ (* i i) i) 1)]
                (when (< idx len)
                  (aset mark idx (byte 1))
                  (recur (+ idx w))))))
          (recur (inc i)))
        (let [p (ArrayList.)]
          (.add p 2)
          (loop [i (int 1)]
            (if (< i len)
              (do
                (when (= (aget mark i) 0)
                  (.add p (+ (bit-shift-left i 1) 1)))
                (recur (+ i 1)))
              p)))))))

(defn firstPrime
  "Return a index of first element that is greater than x, assuming plist is sorted.
   If x is out of bound of plist, return -1.
   Unsorted data will cause -2 error during search."
  ^long [x ^ints plist]
  (let [n (alength plist)
        last-idx (dec n)
        x (int x)]
    (cond
      (= x (aget plist 0))
      0

      (= x (aget plist last-idx))
      last-idx

      (and (> x (aget plist 0)) (< x (aget plist last-idx)))
      (loop [l 0
             r last-idx]
        (let [w (- r l)
              i (+ l (bit-shift-right w 1))
              c (aget plist i)]
          ;; (println " ==> " i ", w = " w)
          (if (< x c)
            (recur l (dec i))
            (if (>= x c)
              (let [i+1 (inc i)]
                (if (< x (aget plist i+1))
                  i+1
                  (recur i+1 r)))
              -2))))
      :else -1)))


(def prime-seq prime-seq4)

(defn prime?
  ([^long n]
   (let [ps ^ints (prime-seq3 n)
         len (alength ps)]
     (= n (aget ps (unchecked-dec-int len)))))
  ([^long n ^ArrayList plist]
   (every? #(not= (rem n ^long %) 0) plist)))

(defn prime
  "Returns largest prime smaller than n"
  ^long [^long n]
  (let [plist ^ArrayList (prime-seq4 n)]
    (.get plist (unchecked-dec (.size plist)))))

(defn- prime-seq1
  ^ArrayList [^long n] ;; looking for prime number within [2, n]
  (let [plist (ArrayList. [2 3 5 7])
        sieve (.subList plist 1 4) ;; no need to check 2 or 5's factor
        max-s (int (Math/sqrt n))
        i ^long (loop [i 11]
                  (if (<= i max-s)
                    (do (if (prime? i sieve)
                          (.add sieve i))
                        (recur (+ i 2)))
                    i))]
    (println "i = " i "sieve size = " (.size sieve))
    (let [full-plist (ArrayList. plist)]
      (loop [i ^long i]
        (if (<= i n)
          (do (if (prime? i sieve)
                (.add full-plist i))
              (recur (+ i 2)))
          full-plist)))))

(defn first-prime [s plist]
  (loop [s s]
    (if-let [[a & rst] s]
      (if (prime? a plist)
        [a rst]
        (recur rst))
      nil)))


(defn find-prime-step [^long n]
  (let [s (long (if (< n 300)
                  (Math/round (* 0.618 n))
                  (Math/round (* 0.618 (double (/ n 12))))))
        plist ^ArrayList (prime-seq s)]
    (loop [i (unchecked-dec (.size plist))]
      (if (= i 0)
        1
        (let [p (long (.get plist i))]
          (if (= (rem n p) 0)
            (recur (unchecked-dec i))
            p))))))

(def ^{:private true :tag 'ints} _primes_ (int-array (prime-seq4 2000000)))

(defn seed-primes [^long n]
  (def ^ints _primes_ (int-array (prime-seq4 n))))


(defn prime-search
  (^long [^long x]
   (let [len (alength _primes_)]
     #_(println "len = " len)
     (loop [a (int 0)
            d (dec len)
            b (- d (Math/round (* len gr)))
            c (+ a (Math/round (* len gr)))]
       ;; (println a b c d)
       (let [plist-c ^int (aget ^ints _primes_ c)
             plist-b ^int (aget ^ints _primes_ b)]
         (cond
           (< (- d a) 2)
           #_[(aget _primes_ a) (aget _primes_ d)]
           (aget _primes_ a)

           (= x plist-b)
           plist-b

           (= x plist-c)
           plist-c

           (< plist-b x)
           (recur b d c (+ b (Math/round (* (- d b) gr))))

           (< x plist-c)
           (recur a c (- c (Math/round (* (- c a) gr))) b)))))))


(defn co-prime
  "Given a i-th prime and integer n, find a prime,
   starting from i-th and downward, that is co-prime with n"
  ^long [^long i ^long n]
  (loop [i i]
    (if (<= 0 i)
      (let [p (aget _primes_ i)]
        (if (= (rem n p) 0)
          (recur (unchecked-dec i))
          p))
      1)))

(defn co-prime-step
  (^long [^long x ^long n]
   (let [len (alength _primes_)]
     #_(println "len = " len)
     (loop [a (int 0)
            d (dec len)
            b (- d (Math/round (* len gr)))
            plist-b ^int (aget ^ints _primes_ b)
            c (+ a (Math/round (* len gr)))
            plist-c ^int (aget ^ints _primes_ c)]
       ;; (println a b c d)
       (cond
         (< (- d a) 2)
         #_[(aget _primes_ a) (aget _primes_ d)]
         (co-prime a n)

         (= x plist-b)
         (co-prime b n)

         (= x plist-c)
         (co-prime c n)

         (< plist-b x)
         (let [next-c (+ b (Math/round (* (- d b) gr)))]
          (recur b d c plist-c next-c (aget _primes_ next-c)))

         (< x plist-c)
         (let [next-b (- c (Math/round (* (- c a) gr)))]
           (recur a c next-b (aget _primes_ next-b) b plist-b)))))))


