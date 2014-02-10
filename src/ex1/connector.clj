;Jani Rahkola
;013606996

(ns ex1.connector
  (:import java.net.Socket
           java.net.ServerSocket
           java.io.IOException
           java.io.EOFException
           java.io.DataInputStream
           java.io.DataOutputStream))

(defn safe-println [& args]
  (locking *out*
    (apply println args)))

(defn- listen [_ in receive]
  (when-let [m (try (.readUTF in)
                    (catch EOFException e
                      nil)
                    (catch Exception e
                      nil))]
    (receive m)
    (send-off *agent* listen in receive)
    true))

(defn- ->Connection [socket in out receive]
  (let [inbox (agent true)]
    {:socket socket
     :out out
     :receiving (send-off inbox listen in receive)}))

(defn- accept-peer [server-socket peer me receive]
  (let [socket (.accept server-socket)
        in (new DataInputStream (.getInputStream socket))
        out (new DataOutputStream (.getOutputStream socket))]
    (if (= (.readLong in) (:id peer))
      (do (.writeLong out (:id me))
          (->Connection socket in out receive))
      (do (.close socket)
          (recur server-socket peer me receive)))))

(defn- connect-to-peer [peer me receive]
  (if-let [socket (try (new Socket (:inet-addr peer) (:port peer))
                       (catch IOException e nil))]
    (let [in (new DataInputStream (.getInputStream socket))
          out (new DataOutputStream (.getOutputStream socket))]
      (.writeLong out (:id me))
      (if-let [_ (try (.readLong in) (catch EOFException e nil))]
        (->Connection socket in out receive)
        (do (.close socket)
            (recur peer me receive))))
    (recur peer me receive)))

(defn connect [server-socket peer me receive]
  (if (< (:id peer) (:id me))
    (future (accept-peer server-socket peer me receive))
    (future (connect-to-peer peer me receive))))

(defn send-message [connection message]
  (let [out (:out @connection)]
    (try (.writeUTF out message)
         (.flush out)
         (catch Exception e))
    connection))

(defn disconnect [connection]
  (.shutdownOutput (:socket @connection))
  (while @(:receiving @connection))
  (.close (:socket @connection)))
