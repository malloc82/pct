(ns pct.plot.cut
  (:use clojure.core)
  (:require pct.data.io
            [oz.core :as oz]
            [clojure.java.javadoc :refer [javadoc]])
  (:import [java.util HashMap]))

(def PORT 20010)

(oz/start-server! PORT)

(defn nth-row
  [^String filename ^long row]
  (with-open [rdr (clojure.java.io/reader filename)]
    (mapv #(java.lang.Double/parseDouble %) (clojure.string/split (nth (line-seq rdr) row) #"\s"))))


(def sample-row 102) ;; 102 55 40

(def data_folder "results/pb005_2021-08-30T03-16-14_-0500")
(def data_folder "results/pb005_2021-08-30T04-33-21_-0500")
(def data_folder "results/pb005_2021-08-30T04-47-41_-0500")
(def data_folder "results/pb005_2021-08-30T16-37-02_-0500")
(def data_folder "results/pb005_2021-08-30T16-44-20_-0500")
(def data_folder "results/pb005_2021-08-30T16-52-10_-0500")
(def data_folder "results/pb005_2021-08-30T17-04-16_-0500")
(def data_folder "results/pb005_2021-08-30T17-07-55_-0500")
(def data_folder "results/pb005_2021-08-30T17-12-50_-0500")
(def data_folder "results/pb005_2021-08-30T17-17-29_-0500")
(def data_folder "results/pb005_2021-08-30T20-16-01_-0500")
(def data_folder "results/pb005_2021-08-30T22-05-42_-0500")
(def data_folder "results/pb005_2021-08-30T22-38-39_-0500")
(def data_folder "results/pb005_2021-08-31T20-56-44_-0500")
(def data_folder "results/pb005_2021-08-31T21-04-36_-0500")
(def data_folder "results/pb005_2021-08-31T21-11-04_-0500")
(def data_folder "results/pb005_2021-08-31T23-50-41_-0500")
(def data_folder "results/pb005_2021-09-01T20-45-54_-0500")

(let [row-sample-iter0 (nth-row "results/x0_george_1mm/x_0_32.txt"     sample-row)
      row-sample-iter1 (nth-row (format "%s/x_01_032.txt" data_folder) sample-row)
      row-sample-iter2 (nth-row (format "%s/x_02_032.txt" data_folder) sample-row)
      row-sample-iter3 (nth-row (format "%s/x_03_032.txt" data_folder) sample-row)
      row-sample-iter4 (nth-row (format "%s/x_04_032.txt" data_folder) sample-row)
      row-sample-iter5 (nth-row (format "%s/x_05_032.txt" data_folder) sample-row)
      row-sample-iter6 (nth-row (format "%s/x_06_032.txt" data_folder) sample-row)
      data-spec
      {:title {:text (format "Cross Section Plot 0 vs 6 iteration at row = %d" sample-row)
               :subtitle (let [config (pct.data.io/load-recon-opts data_folder)
                               lambdas (mapv (fn [[k v]]
                                               (format "%d  :  %.5f" k v))
                                             (into (sorted-map) (:lambda config)))
                               depth (:grid-depth config)]
                           [(if depth
                              (clojure.string/join ",    " (take depth lambdas))
                              (str lambdas))
                            (format "tvs = %s" (:tvs? config))
                            (format "iteration = %s" (:iterations config))
                            (format "runtime = %s" (-> config :properties :runtime))])
               #_(format "lambda = %s test"(str (into (sorted-map) (:lambda (pct.data.io/load-recon-opts data_folder)))))
               :fontSize 20 :color "black"}
       :data {:values (concat (map-indexed (fn [i w] {"column-id" i "wepl" w "iteration" "iteration=0"}) row-sample-iter0)
                              (map-indexed (fn [i w] {"column-id" i "wepl" w "iteration" "iteration=1"}) row-sample-iter1)
                              (map-indexed (fn [i w] {"column-id" i "wepl" w "iteration" "iteration=2"}) row-sample-iter2)
                              (map-indexed (fn [i w] {"column-id" i "wepl" w "iteration" "iteration=3"}) row-sample-iter3)
                              (map-indexed (fn [i w] {"column-id" i "wepl" w "iteration" "iteration=4"}) row-sample-iter4)
                              (map-indexed (fn [i w] {"column-id" i "wepl" w "iteration" "iteration=5"}) row-sample-iter5)
                              (map-indexed (fn [i w] {"column-id" i "wepl" w "iteration" "iteration=6"}) row-sample-iter6)
                              )}
       ;; :mark {:type "bar" :cornerRadiusEnd 4}
       :mark {:type "line"}
       :encoding {:x {:field "column-id"
                      :type  "quantitative" ;; "ordinal" ;; "quantitative";; "ordinal"
                      ;; :scale {:type "linear" :domain [1 15]}
                      :axis  {:title "Column"}}
                  ;; :y {:field "count"
                  ;;     :type  "quantitative"
                  ;;     :axis  {:title "Number of Protons"}}
                  :y {:field "wepl"
                      :type  "quantitative"
                      :axis  {:title "WEPL"}
                      :scale {:type "linear" :domain [0 2.0]}}
                  :color { :field "iteration" :type "nominal"}
                  }

       ;; :layer [{:mark {:type "line" :color "blue" :legend "Iteration 0"}
       ;;          :encoding {:x {:field "column-id"
       ;;                         :type  "quantitative" ;; "ordinal" ;; "quantitative";; "ordinal"
       ;;                         ;; :scale {:type "linear" :domain [1 15]}
       ;;                         :axis  {:title "Column"}}
       ;;                     ;; :y {:field "count"
       ;;                     ;;     :type  "quantitative"
       ;;                     ;;     :axis  {:title "Number of Protons"}}
       ;;                     :y {:field "wepl-0"
       ;;                         :type  "quantitative"
       ;;                         :axis  {:title "WEPL"}
       ;;                         :scale {:type "linear" :domain [0 2.0]}}
       ;;                     :color { :field "color" :legend {:title "hi"}}
       ;;                     }}
       ;;         {:mark {:type "line" :color "red" :legend "Iteration 6"}
       ;;          :encoding {:x {:field "column-id"
       ;;                         :type  "quantitative" ;; "ordinal" ;; "quantitative";; "ordinal"
       ;;                         ;; :scale {:type "linear" :domain [1 15]}
       ;;                         :axis  {:title "Column"}}
       ;;                     ;; :y {:field "count"
       ;;                     ;;     :type  "quantitative"
       ;;                     ;;     :axis  {:title "Number of Protons"}}
       ;;                     :y {:field "wepl-1"
       ;;                         :type  "quantitative"
       ;;                         :axis  {:title "WEPL"}
       ;;                         :scale {:type "linear" :domain [0 2.0]}}
       ;;                     :color { :field "color" :legend {:title "you"}}
       ;;                     }}]
       :width 800
       :height 800
       }]

  (oz/view! data-spec :port PORT))
