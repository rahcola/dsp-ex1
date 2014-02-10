;Jani Rahkola
;013606996

(ns ex1.vclock
  (:refer-clojure :exclude [empty merge]))

(defn empty [ids]
  (into {} (for [id ids] [id 0])))

(defn increment [vclock id]
  (assoc vclock id (inc (get vclock id 0))))

(defn merge [vclock vclock']
  (merge-with max vclock vclock'))
