(ns pct.util.system
  (:require pct.logging taoensso.timbre [clojure.pprint :refer [pprint]])
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

(defonce ^{:tag 'int}  PhysicalCores (.getPhysicalProcessorCount cpu))
(defonce ^{:tag 'int}  LogicalCores  (.getLogicalProcessorCount  cpu))
(defonce ^{:tag 'long} MaxHeapSize   (.maxMemory (Runtime/getRuntime)))
(defonce ^{:tag 'long} MaxMemory     (-> hw .getMemory .getTotal))

(defonce ^:private log-chan (clojure.core.async/chan (+ pct.util.system/LogicalCores 1024)))



(taoensso.timbre/merge-config! {:timestamp-opts {:pattern "yyyy-MM-dd @ HH:mm:ss Z"
                                                 :locale  :jvm-default
                                                 :timezone (java.util.TimeZone/getTimeZone "America/Chicago")}
                                :appenders
                                { ;; :println (timbre/println-appender {:stream :auto})
                                 :println nil
                                 ;; :spit (appenders/spit-appender {:fname "./log/timbre-spit.log"})
                                 ;; :spit (rotor/rotor-appender {:path "./log/messages.log"})
                                 :spit (pct.logging/async-appender {:channel log-chan :path "./log/messages.log"})
                                 }})

;; exception handler setting for core.async threads
(Thread/setDefaultUncaughtExceptionHandler
 (reify Thread$UncaughtExceptionHandler
   (uncaughtException [_ thread ex]
     (taoensso.timbre/error ex (format "Uncaught exception on [%s]" (.getName thread))))))

(defn get-timestamp [] (.format (java.text.SimpleDateFormat. "YYYY-MM-dd'T'HH-mm-ss_Z") (java.util.Date.)))

(defn timestamp [] (.format (java.text.SimpleDateFormat. "YYYY-MM-dd'T'HH-mm-ss_Z") (java.util.Date.)))

(defn ns-all-vars
  "List all vars of a given namespace"
  [ns]
  (filterv #(and (instance? clojure.lang.Var %)
                 (= ns (.getName ^clojure.lang.Namespace (.ns ^clojure.lang.Var %))))
           (vals (ns-map ns))))

(defn ns-publics-list [ns]
  (list (ns-name ns) (keys (ns-publics ns))))

(defn ns-clear-vars
  "Clear all variables of a given namespace"
  ([]
   (let [ns (ns-name *ns*)]
     (println "using current namespace: " ns)
     (ns-clear-vars ns)))
  ([ns]
   (doseq [[k v] (ns-map ns)]
     (when (and (instance? clojure.lang.Var v)
                (= ns (.getName ^clojure.lang.Namespace (.ns ^clojure.lang.Var v))))
       ;; (println k)
       (ns-unmap ns k)))
   #_(let [it (clojure.lang.RT/iter (keys (ns-publics ns)))]
     (loop []
       (when (.hasNext it)
         (let [var (.next it)]
          (println var)
          (ns-unmap ns var)
          (recur)))))))

(defn localhost []
  (java.net.InetAddress/getLocalHost))

(defn hostname []
  (.getHostName (java.net.InetAddress/getLocalHost)))

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
