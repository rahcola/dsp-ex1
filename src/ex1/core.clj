(ns ex1.core
  (:require [clojure.java.io :refer [reader]])
  (:import java.net.InetAddress
           java.net.Socket
           java.net.ServerSocket
           java.net.ConnectException
           java.io.DataInputStream
           java.io.DataOutputStream))

(defn read-config-line [line]
  (let [[id host port] (.split line " ")]
    [(Long/valueOf id)
     {:inet-addr (InetAddress/getByName host)
      :port (Integer/parseInt port)}]))

(defn read-config [file-reader]
  (doall (into {} (map read-config-line (line-seq file-reader)))))

(defn create-server-socket [me]
  (new ServerSocket (:port (val me))))

(defn accept-peers [server-socket others]
  (loop [n (count others)
         others others]
    (if (zero? n)
      others
      (let [s (.accept server-socket)
            id (.readLong (new DataInputStream (.getInputStream s)))]
        (recur (dec n)
               (assoc-in others [id :socket] s))))))

(defn loop-connect [{:keys [inet-addr port] :as peer}]
  (if-let [s (try [(new Socket inet-addr port)]
                  (catch ConnectException e
                    nil))]
    (first s)
    (recur peer)))

(defn connect-to-peer [me peer]
  (with-open [s (loop-connect (val peer))]
    (.writeLong (new DataOutputStream (.getOutputStream s)) (key me))))

(defn connect-to-peers [me others]
  (doseq [peer others]
    (connect-to-peer me peer)))

(defn -main [& args]
  (let [[config-file id] args
        id (Long/valueOf id)
        config (with-open [r (reader config-file)]
                 (read-config r))
        me (first (filter #(= id (key %)) config))
        others (into {} (remove #(= id (key %)) config))]
    (.start (new Thread (fn [] (connect-to-peers me others))))
    (println (accept-peers (create-server-socket me) others))))
