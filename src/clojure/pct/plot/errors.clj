(ns pct.plot.errors
  (:use clojure.core)
  (:require pct.data.io
            [oz.core :as oz]
            [clojure.java.javadoc :refer [javadoc]]
            [com.rpl.specter :as s])
  (:import [java.util HashMap ArrayList]))

(def PORT 20020)

(oz/start-server! PORT)

(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-33-29_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-32-06_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-30-41_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-29-17_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-27-54_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-26-28_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-25-04_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-23-40_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-22-17_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-20-54_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-19-29_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50/pb005_2021-09-02T00-19-29_-0500/recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50//recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50//recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50//recon_config.json"))
(def recon-result (pct.data.io/load-data "results/50//recon_config.json"))


(let [data (ArrayList.)]
  (doseq [[iter series] (-> recon-result :result :stats)]
    (doseq [[region stats] series]
      (.add data (merge {:iter (long iter) :region (name region)}
                        (let [error (:error stats)]
                          (assoc stats :error (Math/abs error)))))))
  (let [spec {:title {:text (format "lambda = %.5f, %.5f, %.5f, ... "
                                    (-> recon-result :lambda (get 1))
                                    (-> recon-result :lambda (get 2))
                                    (-> recon-result :lambda (get 3)))}
              :data {:values (remove #(= (:region %) "sinus") (vec data))}
              :encoding {:x {:field "iter"
                             :type "nominal"
                             :axis {:title "iteration"}}
                         :y {:field "error"
                             :type "quantitative"
                             :axis {:title "error, %"}}
                         :facet {:field "region"
                                 :type "ordinal"}
                         :color {:field "region"}}
              :mark {:type "bar"}
              :width 100 :height 500}]
    (oz/view! spec :port PORT)))





