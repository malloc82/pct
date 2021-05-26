(ns pct.data_api_test
  (:use clojure.core)
  (:require pct.data pct.data.io pct.async.threads
            [uncomplicate.neanderthal
             [core :refer :all]
             [block :refer [buffer contiguous?]]
             [native :refer :all]]
            [uncomplicate.neanderthal.auxil :refer :all]
            [uncomplicate.neanderthal.internal
             [api :as api]]
            [uncomplicate.neanderthal.internal.host
             [mkl :as mkl]]
            [uncomplicate.commons.core :refer [release with-release releaseable? let-release info]]))



#_(def dataset_0 (pct.data.io/newPCTDataset
                {:rows   200
                 :cols   200
                 :slices 16
                 :dir    "datasets/data_4_Ritchie/exp_CTP404/B_1280000_L_1.000000_359"
                 :path   "MLP_paths_r=0.bin"
                 :b      "WEPL.bin"}))


(def dataset_1 (pct.data.io/newPCTDataset
                {:rows   200
                 :cols   200
                 :slices 16
                 :dir    "datasets/data_4_Ritchie_10_24_2019"
                 :path   "MLP_paths_r=1.bin"
                 :b      "WEPL.bin"}))



;; read header

;; (def ^pct.data.HistoryInputStream in-stream_0 (pct.data/newHistoryInputStream (:path dataset_0) (:b dataset_0)))
;; (def ^pct.data.HistoryInputStream in-stream_1 (pct.data/newHistoryInputStream (:path dataset_1) (:b dataset_1)))


;; consistency read test

(def res (pct.data.io/load-dataset dataset_1 {:min-len 15 :batch-size 50000}))

;; read history index
