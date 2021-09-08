(ns pct.plot.slice_count
  (:use clojure.core)
  (:require pct.data.io
            [oz.core :as oz]
            [clojure.java.javadoc :refer [javadoc]]))

(def PORT1 20000)
(def PORT2 20001)

(oz/start-server! PORT1)
(oz/start-server! PORT2)

(def data (pct.data.io/load-plot-data "plot-data/george_1mm_distribution.json"))

(let [spec
      {:data {:values (apply concat
                             (map (fn [[slice-idx counts]]
                                    (map-indexed (fn [i c] {:x i :y c :slice (format "slice %02d" slice-idx)}) counts))
                                  (into (sorted-map) (:data data))))}
       :mark "bar"
       :encoding {:x {:field "x"
                      :type "quantitative"
                      :axis {:title "slices"}}
                  :y {:field "y" #_#_:aggregate "sum"
                      :type "quantitative"
                      :axis {:title "count"}}
                  ;; :color {:field "slice"
                  ;;         :type "nominal"}
                  #_#_:row {:field "slice"
                        :type "nominal"
                            :title "slice"}
                  :facet {:field "slice"
                          :type "ordinal"
                          :columns 11}}
       :width 120 :height 70}]
  (oz/view! spec :port PORT1))


(let [data (reduce (fn [^longs acc [_ counts]]
                                 (let [n (count counts)
                                       a (alength acc)
                                       ^longs acc (if (< a n)
                                                    (let [new-acc (long-array n)]
                                                      (System/arraycopy acc 0 new-acc 0 a)
                                                      new-acc)
                                                    acc)]
                                   (loop [i (long 0)]
                                     (if (< i n)
                                       (do (aset acc i (+ (aget acc i) (counts i)))
                                           (recur (unchecked-inc i)))
                                       acc))))
                               (long-array 1)
                               (:data data))
      total (reduce + data)
      spec
      {:data {:values (map-indexed
                       (fn [i v]
                         {:x i :y v "percentage" (format "%.1f%%" (* (/ (double v) total) 100))})
                       data)}
       :encoding {:x {:field "x"
                      :type "quantitative"
                      :axis {:title "slices"}}
                  :y {:field "y"
                      :type "quantitative"
                      :axis {:title "count"}}}
       :layer [{:mark "bar"}
               {:mark {:type "text" ;; for more see https://vega.github.io/vega-lite/docs/text.html
                       ;; :align "middle"
                       ;; :baseline "top"
                       ;; :dx 0
                       :dy -8
                       :fontWeight "bold"}
                :encoding {:text {:field "percentage", :type "nominal", }}}]
       :width 1000 :height 1000}]
  (println total)
  (oz/view! spec :port PORT2))


