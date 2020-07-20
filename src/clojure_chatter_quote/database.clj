(ns clojure-chatter-quote.database
  (:require [datomic.api :as d]))

(def uri "datomic:mem://chatter-quote")

(defn make-db [] (d/create-database uri))
(defn delete-db [] (d/delete-database uri))

(defn ensure-schema-exists
  "Sets up the schema to hold the quotes.
  Surely there's a right way to manage this."
  []
  (let [conn (d/connect uri)]
    (do
      (d/transact conn [{:db/id (d/tempid :db.part/db)
                         :db/ident :quote/text
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/doc "A quote being managed - unique"
                         :db/unique :db.unique/value
                         :db.install/_attribute :db.part/db}])
      (d/transact conn [{:db/id (d/tempid :db.part/db)
                         :db/ident :quote/tag
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/many
                         :db/doc "Tags for quotes to make them easy to schedule"
                         :db.install/_attribute :db.part/db}])
      (d/transact conn [{:db/id (d/tempid :db.part/db)
                         :db/ident :schedule/name
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/doc "Name of this sending schedule - used for internal
                                  organization and also to easily identify the schedule - unique"
                         :db/unique :db.unique/value
                         :db.install/_attribute :db.part/db}])
      (d/transact conn [{:db/id (d/tempid :db.part/db)
                         :db/ident :schedule/cron
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one
                         :db/doc "The cron expression for this sending schedule"
                         :db.install/_attribute :db.part/db}])
      (d/transact conn [{:db/id (d/tempid :db.part/db)
                         :db/ident :schedule/tag
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/many
                         :db/doc "Quotes to send as part of this schedule in the form of tags"
                         :db.install/_attribute :db.part/db}])
      )))

(defn init
  "Initializes the in-memory database so it's ready to use by the engine."
  []
  (do (make-db) (ensure-schema-exists)))

(defn -transact
  "Internal helper for simplifying datomic transactions
  using the pattern I found to work in this module."
  [statements]
  (let [conn (d/connect uri)]
    (d/transact conn statements)))

(defn -insert
  "Internal helper for simplifying inserting facts into
  the user partition. Pass a map. Each entry will become
  an attribute of the same, new entity."
  [datoms]
  (-transact [(merge datoms {:db/id (d/tempid :db.part/user)})]))

(defn all-quotes
  "Gets all the quotes currently in the database.
  Returns a sequence of maps with keys:
    :id   - primary key
    :text - the actual quote
    :tags - a list of tags associated to this quote"
  []
  (let [conn (d/connect uri)
        texts (d/q '[:find ?qid ?text :where [?qid :quote/text ?text]]
                (d/db conn))
        tags (d/q '[:find ?qid ?tag :where [?qid :quote/tag ?tag]]
               (d/db conn))
        tagsById (group-by (fn [[id tag]] id) tags)]
    (map (fn [[id text]]
           {:id id
            :text text
            :tags (map second (tagsById id))})
      texts)))

(defn quotes-with-tag
  "Gets all the quotes in the database with any of the given tags.
  The argument must be a sequence of tags.
  Returns maps as described in all-quotes."
  [tags]
  (filter (fn [quote] (some (set tags) (:tags quote))) (all-quotes)))

(defn add-quote
  "Adds a quote with tags to the database. Doesn't allow
  duplicate quote text due to schema uniqueness constraint."
  [quote-text tags]
  (-insert {:quote/text quote-text :quote/tag tags}))

(defn remove-quote
  "Unwritten"
  []
  (throw (UnsupportedOperationException. "Can't remove quotes yet")))

(defn tag-quote
  "Adds one or more tags to an existing quote.
  First arg is the quote text.
  Second arg is the sequence of tags to add"
  [text tags]
  (let [conn (d/connect uri)
        quote-id (first (first (d/q '[:find ?qid :in $ ?text :where [?qid :quote/text ?text]]
                                 (d/db conn) text)))]
    (d/transact conn [{:db/id quote-id :quote/tag tags}])))

(defn untag-quote
  "Removes one or more tags from an existing quote.
  Args are like those for tag-quote."
  [text tags]
  (let [conn (d/connect uri)
        quote-id (first (first (d/q '[:find ?qid :in $ ?text :where [?qid :quote/text ?text]]
                                 (d/db conn) text)))]
    (d/transact conn [[:db/retract quote-id :quote/tag tags]])))

(defn add-schedule
  "Stores a quote-sending schedule.
  First arg is the schedule name - uniqueness enforced by the schema.
  Second arg is the cron expression of the schedule.
  Third arg is a sequence of tags to use for this schedule."
  [name cron tags]
  (-insert {:schedule/name name :schedule/cron cron :schedule/tag tags}))

(defn remove-schedule
  "Removes the schedule with the given name from the database."
  [schedule-name]
  (let [conn (d/connect uri)
        schedule-id (first (first
                             (d/q '[:find ?sid :in $ ?name :where [?sid :schedule/name ?name]]
                               (d/db conn) schedule-name)))]
    (d/transact conn [[:db/retract schedule-id :schedule/name schedule-name]])))

(defn all-schedules
  "Gets all the sending schedules currently in the database.
  Returns a sequence of maps with keys:
    :name - the schedule's unique name
    :cron - the cron expression that the schedule runs
    :tags - the quote tags that this schedule is associated with: i.e. identifes the quotes that will be sent on this schedule"
  []
  (let [conn (d/connect uri)
        schedules (d/q '[:find ?name ?cron :where [?sid :schedule/name ?name] [?sid :schedule/cron ?cron]] (d/db conn))
        tags (d/q '[:find ?name ?tag :where [?sid :schedule/name ?name] [?sid :schedule/tag ?tag]] (d/db conn))
        tagsByName (group-by first tags)]
    (map (fn [[name cron]]
           {:name name
            :cron cron
            :tags (->> name tagsByName (map second))}) schedules)))