(ns factorhouse.kafka.topic
  (:require
   [factorhouse.kafka.topic :as topic]))

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

(defn- sorted-group-by [sizes grouping-key]
  (->> sizes
       (group-by grouping-key)
       (into (sorted-map))))

(defn- format-row [id name size parent]
  (let [base-row {:id id :name name :size size}]
    (if (nil? parent) base-row (conj base-row {:parent parent}))))

(defn- format-partitions [topic-id partitions-per-topic]
  (->> (sorted-group-by partitions-per-topic :partition)
       (map-indexed (fn [idx [partition-name partition-details]]
                      (let [partition-id (str idx)
                            partition-size (reduce + (map :size partition-details))]
                        (format-row (str topic-id "." partition-id) partition-name partition-size topic-id))))))

(defn- transform-sizes [sizes]
  (->> (sorted-group-by sizes :topic)
       (map-indexed
        (fn [idx [topic partitions]]
          (let [topic-id (str idx)
                topic-size (reduce + (map :size partitions))
                parent-details (format-row topic-id topic topic-size nil)
                formatted-partitions (format-partitions topic-id partitions)]
            (cons parent-details formatted-partitions))))
       (apply concat)
       vec))

(defn categories-logical
  "Transform topic sizes into categorised logical view"
  [sizes] (transform-sizes sizes))

(defn categories-physical
  "Transform topic sizes into categorised physical view"
  [sizes])

