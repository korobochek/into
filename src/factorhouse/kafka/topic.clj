(ns factorhouse.kafka.topic)

;; Technical Challenge! Implement this function.

;; ASSUMPTION:
;; given the statement in data file: 
;; "* Each Broker having a single '/kfk' directory"
;; there will be always just one directory and it will always be called '/kfk'
(defn- format-topics [topics]
  (mapcat
   (fn [[broker, dir-data]]
     (map #(conj (apply merge %) {:broker broker :dir "/kfk"})
          (get-in dir-data ["/kfk" :replica-infos])))
   topics))

(defn sizes
  "Transform raw topic information into a more useable sizes shape"
  [topics] (vec (sort-by (juxt :broker :topic :partition) (format-topics topics))))


;; Extension Challenge! Implement these functions.

(defn categories-logical
  "Transform topic sizes into categorised logical view"
  [sizes])

(defn categories-physical
  "Transform topic sizes into categorised physical view"
  [sizes])