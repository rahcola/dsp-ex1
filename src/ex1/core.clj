;Jani Rahkola
;013606996

(ns ex1.core
  (:gen-class)
  (:require [ex1.vclock :as vclock]
            [ex1.connector :as connector]
            [clojure.java.io :refer [reader]])
  (:import java.net.InetAddress
           java.net.ServerSocket))

(defn safe-println [& args]
  (locking *out*
    (apply println args)))

(defn read-config-line [^String line]
  (let [[^String id host port] (.split line " ")]
    {:id (Long/valueOf id)
     :inet-addr (InetAddress/getByName host)
     :port (Integer/parseInt port)}))

(defn read-config [file-reader]
  (doall (map read-config-line (line-seq file-reader))))

(defn action-send [vclock remote-id connection my-id]
  (send-off vclock
            (fn [vclock]
              (let [vclock (vclock/increment vclock my-id)]
                (connector/send-message connection (str vclock))
                (safe-println "s" remote-id (vec (vals vclock)))
                vclock))))

(defn action-tick [vclock my-id n]
  (send-off vclock
            (fn [vclock]
              (let [vclock (nth (iterate #(vclock/increment % my-id)
                                         vclock)
                                n)]
                (safe-println "l" n)
                vclock))))

(defn receive [vclock my-id remote-id message]
  (send-off vclock
            (fn [vclock]
              (let [vclock' (read-string message)
                    vclock (-> vclock
                               (vclock/increment my-id)
                               (vclock/merge vclock'))]
                (safe-println "r" remote-id
                              (vec (vals vclock'))
                              (vec (vals vclock)))
                vclock))))

(defn -main [& args]
  (let [[config-file ^String my-id] args
        my-id (Long/valueOf my-id)
        config (with-open [r (reader config-file)]
                 (read-config r))
        port (:port (first (filter #(= my-id (:id %)) config)))
        peers (remove #(= my-id (:id %)) config)
        server-socket (new ServerSocket port)
        vclock (agent (vclock/empty (map :id config)))
        connections (for [peer peers
                          :let [r (partial receive vclock my-id (:id peer))]]
                      [(:id peer)
                       (connector/connect server-socket peer {:id my-id} r)])]
    (doseq [[id c] connections] @c)
    (dotimes [_ 100]
      (if (zero? (rand-int 2))
        (let [n (inc (rand-int 5))]
          (action-tick vclock my-id n))
        (let [[id c] (rand-nth connections)]
          (action-send vclock id c my-id))))
    (await vclock)
    (doseq [[id c] connections]
      (connector/disconnect c))
    (await vclock)
    (shutdown-agents)))
