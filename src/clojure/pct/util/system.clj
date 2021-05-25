(ns pct.util.system
  (:require [clojure.pprint :refer [pprint]])
  (:import (java.awt.datatransfer DataFlavor Transferable StringSelection)
           (java.awt Toolkit)
           (java.io StringWriter)
           [oshi SystemInfo]
           [oshi.hardware HardwareAbstractionLayer CentralProcessor]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


;; doc: https://oshi.github.io/oshi/oshi-core/apidocs/
(defonce ^:private ^HardwareAbstractionLayer hw (.getHardware (SystemInfo.)))
(defonce ^:private ^CentralProcessor cpu (.getProcessor hw))

(defonce ^int  PhysicalCores (.getPhysicalProcessorCount cpu))
(defonce ^int  LogicalCores  (.getLogicalProcessorCount  cpu))
(defonce ^long MaxHeapSize   (.maxMemory (Runtime/getRuntime)))
(defonce ^long MaxMemory     (-> hw .getMemory .getTotal))

(defn get-timestamp [] (.format (java.text.SimpleDateFormat. "YYYY-MM-dd'T'HH-mm-ss_Z") (java.util.Date.)))

(defn ns-all-vars[n]
  (filter (fn[[_ v]]
            (and (instance? clojure.lang.Var v)
                 (= n (.getName ^clojure.lang.Namespace (.ns ^clojure.lang.Var v)))))
          (ns-map n)))

(defn ns-publics-list [ns] (#(list (ns-name %) (map first (ns-publics %))) ns))

(defn localhost []
  (java.net.InetAddress/getLocalHost))

(defn readableFormat [n]
  (let [iter (clojure.lang.RT/iter ["B" "KB" "MB" "GB" "TB"])]
   (loop [n ^double (double n)
          unit (.next iter)]
     (if (or (< n 1024.0) (not (.hasNext iter)))
       [n unit]
       (recur (/ n 1024.0) (.next iter))))))

;; Source: https://gist.github.com/baskeboler/7d226374582246d28b25801e28e18216
(defn ^java.awt.datatransfer.Clipboard get-clipboard
  "get system clipboard"
  []
  (-> (Toolkit/getDefaultToolkit)
      (.getSystemClipboard)))

(defn slurp-clipboard
  "get latest string from clipboard"
  []
  (when-let [^Transferable clip-text (some-> (get-clipboard)
                                             (.getContents nil))]
    (when (.isDataFlavorSupported clip-text DataFlavor/stringFlavor)
      (->> (.getTransferData clip-text DataFlavor/stringFlavor)
           (cast String)))))

(defn spit-clipboard
  "write string s to clipboard"
  [s]
  (let [sel (StringSelection. s)]
    (some-> (get-clipboard)
            (.setContents sel sel))))

; an alias for spit
(def  str->clipboard spit-clipboard)

(defn  pprint-data-to-clipbaord
  "pretty prints a data structure into the clipboard"
  [d]
  (let [wr (java.io.StringWriter.)]
    (pprint d wr)
    (str->clipboard (.toString wr))))
