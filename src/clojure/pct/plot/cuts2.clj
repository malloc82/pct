(ns pct.plot.cuts2
  (:use clojure.core)
  (:require [pct.data.io :refer [load-data]]
            [oz.core :as oz]
            [clojure.java.javadoc :refer [javadoc]])
  (:import [java.util HashMap ArrayList]))

(def PORT  20010)
(def PORT2 20010)

(oz/start-server! PORT)
(oz/start-server! PORT2)

(defn nth-row
  [^String filename ^long row]
  (with-open [rdr (clojure.java.io/reader filename)]
    (mapv #(java.lang.Double/parseDouble %) (clojure.string/split (nth (line-seq rdr) row) #"\s"))))

(def sample-row 102) ;; 102 55 40

(let [folders    (HashMap.)
      cut-groups (HashMap.)]
  (doseq [fname  (-> (apply clojure.java.shell/sh (clojure.string/split "find results -type f -iname x_*_033.txt" #"\s+"))
                     :out
                     (clojure.string/split #"\s+"))]
    (let [folder_name (-> (java.io.File. fname)
                          (.getParent))
          iter (java.lang.Long/parseLong (second (re-find #".*/x_(\d+)_.*\.txt" fname)))]
      (if-let [f-list (.get folders folder_name)]
        (.put folders folder_name (assoc f-list iter fname))
        (.put folders folder_name (into (sorted-map) [[iter fname]])))
      (if-let [min-len (re-find #"results/(\d+)/.*" folder_name)]
        (let [k (java.lang.Long/parseLong (second min-len))]
          (if-let [__folders (.get cut-groups k)]
            (.put cut-groups k (conj __folders folder_name))
            (.put cut-groups k (into (sorted-set) [folder_name])))))))
  (def folders (into {} folders))
  (def groups  (into (sorted-map) cut-groups)))


(defn dataset-from-run
  [^String folder_name ^long row]
  (let [recon-opts (load-data (format "%s/recon_config.json" folder_name))
        lambda (:lambda recon-opts)
        min-len (-> recon-opts :dataset :min-len)
        lambda_setting (format "min-len = %03d, lambda = %.5f, %.5f, %.5f, ..." min-len (lambda 1) (lambda 2) (lambda 3))
        x0 (map-indexed (fn [i w] {"col" i "wepl" w "iter" (format "iteration=%d" 0)
                                  "lambda" lambda_setting
                                  #_#_"min-len" min-len})
                        (nth-row "results/x0_george_1mm/x_0_33.txt" row))
        formatted-data (->> (folders folder_name)
                            (mapcat (fn [[iter filename]]
                                      (map-indexed (fn [i w] {"col" i "wepl" w "iter" (format "iteration=%02d" iter)
                                                             "lambda" lambda_setting
                                                             #_#_"min-len" min-len})
                                                   (nth-row filename row))))
                            (concat x0))]
    formatted-data))


(defn dataset-from-index
  [min-len row]
  (mapcat #(dataset-from-run % row) (groups min-len)))


(defn errors-from-run
  [^String folder_name]
  (let [recon-result (load-data (format "%s/recon_config.json" folder_name))
        lambda (:lambda recon-opts)
          min-len (-> recon-opts :dataset :min-len)
        data (ArrayList.)]
    (doseq [[iter series] (-> recon-result :result :stats)]
      (doseq [[region stats] series]
        (.add data (merge {:iter (long iter) :region (name region)}
                          (let [error (:error stats)]
                            (assoc stats :error (Math/abs error)))))))
    (let [
          lambda_setting (format "min-len = %03d, lambda = %.5f, %.5f, %.5f, ..." min-len (lambda 1) (lambda 2) (lambda 3))
          stats (-> recon-opts :result :stats)
          errors (merge {:iter (long iter) :region (name region)}
                        (-> recon-opts :result :stats))]
      (assoc errors "lambda" lambda_setting))))


(defn errors-from-index
  [min-len]
  (mapcat #(errors-from-run %) (groups min-len)))


;; "results/x0_george_1mm/x_0_33.txt"
(let [ ;; folder_name "results/100/pb005_2021-09-02T02-56-37_-0500"
      ROW  55
      spec {:data {:values (mapcat #(dataset-from-index % ROW) (keys groups))
                   #_(dataset-from-index 100 ROW)
                   #_(dataset-from-run "results/100/pb005_2021-09-02T02-56-37_-0500" ROW)}
            :mark {:type "line"}
            :encoding {:x {:field "col"
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
                       :facet {:field "lambda"
                               :type "ordinal"
                               :columns 22
                               :rows (count (keys groups))}
                       :color { :field "iter" :type "nominal"}
                       }
            :width 500 :height 500}]
  ;;(tap> data)
  (oz/view! spec :port PORT))

#_(let [;; folder_name "results/100/pb005_2021-09-02T02-56-37_-0500"
      spec {:data {:values (mapcat #(errors-from-index %) (keys groups))
                   #_(dataset-from-index 100 ROW)
                   #_(dataset-from-run "results/100/pb005_2021-09-02T02-56-37_-0500" ROW)}
            :mark {:type "line"}
            :encoding {:x {:field "iter"
                           :type  "quantitative" ;; "ordinal" ;; "quantitative";; "ordinal"
                           ;; :scale {:type "linear" :domain [1 15]}
                           :axis  {:title "iterations"}}
                       ;; :y {:field "count"
                       ;;     :type  "quantitative"
                       ;;     :axis  {:title "Number of Protons"}}
                       :y {:field "error"
                           :type  "quantitative"
                           :axis  {:title "error, %"}}
                       :facet {:field "lambda"
                               :type "ordinal"
                               :columns 22
                               :rows (count (keys groups))}
                       :color { :field "iter" :type "nominal"}}
            :width 500 :height 500}]
  ;;(tap> data)
  (oz/view! spec :port PORT2))

