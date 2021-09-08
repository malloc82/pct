(ns pct.plot.distribution_test
  (:use clojure.core)
  (:require [pct.util.distributions :refer [poisson-distribution rayleigh-distribution gamma-distribution]]
            [oz.core :as oz]))


(def PORT 19990)
(oz/start-server! PORT)

;; poisson distribution

(let [f1  (poisson-distribution 1)
      f4  (poisson-distribution 4)
      f10 (poisson-distribution 10)
      spec
      {:data {:values (let [x (range 21)]
                        (concat
                         (map (fn [i] {:x i :y (f1 i)  :label "lambda=1"})  x)
                         (map (fn [i] {:x i :y (f4 i)  :label "lambda=4"})  x)
                         (map (fn [i] {:x i :y (f10 i) :label "lambda=10"}) x)))}
       :mark {:type "line" :point true}
       :encoding {:x {:field "x" :type "quantitative"
                      :axis {:title "k"}}
                  :y {:field "y" :type "quantitative"
                      :axis {:title "P( x = k)"}}
                  :color {:field "label" :type "nominal"}}
       :width 800 :height 800
       :description "test"}]

  (oz/view! spec :port PORT :description "test"))


(let [f0p5 (rayleigh-distribution 0.5)
      f1   (rayleigh-distribution 1)
      f2   (rayleigh-distribution 2)
      f3   (rayleigh-distribution 3)
      f4   (rayleigh-distribution 4)
      spec
      {:data {:values (let [x (range 0 10 0.01)]
                        (concat
                         (map (fn [i] {:x i :y (f0p5 i)  :label "theta=0.5"}) x)
                         (map (fn [i] {:x i :y (f1 i)    :label "theta=1"})   x)
                         (map (fn [i] {:x i :y (f2 i)    :label "theta=2"})   x)
                         (map (fn [i] {:x i :y (f3 i)    :label "theta=3"})   x)
                         (map (fn [i] {:x i :y (f4 i)    :label "theta=4"})   x)))}
       :mark {:type "line" :point false}
       :encoding {:x {:field "x" :type "quantitative"
                      #_#_:axis {:title "k"}}
                  :y {:field "y" :type "quantitative"
                      #_#_:axis {:title "P( x = k)"}}
                  :color {:field "label" :type "nominal"}}
       :width 800 :height 800
       :description "test"}]

  (oz/view! spec :port PORT :description "test"))


(let [f_1_2   (gamma-distribution 1.0 2.0)
      f_2_2   (gamma-distribution 2.0 2.0)
      f_3_2   (gamma-distribution 3.0 2.0)
      f_5_1   (gamma-distribution 5.0 1.0)
      f_9_0p5 (gamma-distribution 9.0 0.5)
      f_7p5_1 (gamma-distribution 7.5 1.0)
      f_0p5_1 (gamma-distribution 0.5 1.0)
      spec
      {:data {:values (let [x (range 0 20 0.01)]
                        (concat
                         (map (fn [i] {:x i :y (f_1_2   i)  :label "k=1.0, theta=2.0"}) x)
                         (map (fn [i] {:x i :y (f_2_2   i)  :label "k=2.0, theta=2.0"}) x)
                         (map (fn [i] {:x i :y (f_3_2   i)  :label "k=3.0, theta=2.0"}) x)
                         (map (fn [i] {:x i :y (f_5_1   i)  :label "k=5.0, theta=1.0"}) x)
                         (map (fn [i] {:x i :y (f_9_0p5 i)  :label "k=9.0, theta=0.5"}) x)
                         (map (fn [i] {:x i :y (f_7p5_1 i)  :label "k=7.5, theta=1.0"}) x)
                         (map (fn [i] {:x i :y (f_0p5_1 i)  :label "k=0.5, theta=1.0"}) (range 0.4 20 0.001))))}
       :mark {:type "line" :point false}
       :encoding {:x {:field "x" :type "quantitative"
                      #_#_:axis {:title "k"}}
                  :y {:field "y" :type "quantitative"
                      #_#_:axis {:title "P( x = k)"}}
                  :color {:field "label" :type "nominal"}}
       :width 800 :height 800
       :description "test"}]

  (oz/view! spec :port PORT :description "test"))





