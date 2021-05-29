(ns pct.data_api_test
  (:use clojure.core pct.common)
  (:require pct.data pct.data.io pct.async.threads
            [clojure.core.async :as a]
            [com.rpl.specter :as spr]
            [uncomplicate.neanderthal
             [core :refer :all]
             [block :refer [buffer contiguous?]]
             [native :refer :all]]
            [uncomplicate.neanderthal.auxil :refer :all]
            [uncomplicate.neanderthal.internal
             [api :as api]]
            [uncomplicate.neanderthal.internal.host
             [mkl :as mkl]]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]])
  (:import [java.util ArrayList Scanner Arrays]
           [java.io File]))


(def test-result (with-open [dataset (pct.data.io/newPCTDataset
                                      {:rows   200
                                       :cols   200
                                       :slices 16
                                       :dir    "datasets/data_4_Ritchie/exp_CTP404/10_24_2019"
                                       :path   "MLP_paths_r=1.bin"
                                       :b      "WEPL.bin"})]
                   (pct.data.io/test-dataset dataset)))

(def data-index (with-open [dataset (pct.data.io/newPCTDataset
                                     {:rows   200
                                      :cols   200
                                      :slices 16
                                      :dir    "datasets/data_4_Ritchie/exp_CTP404/10_24_2019"
                                      :path   "MLP_paths_r=1.bin"
                                      :b      "WEPL.bin"})]
                  (pct.data.io/load-dataset dataset {:min-len 30 :batch-size 75000})))

(def data-distribution
  (mapv (fn [i]
          [i (reduce + (spr/transform spr/ALL count (data-index i)))])
        (range (.slices data-index))))
