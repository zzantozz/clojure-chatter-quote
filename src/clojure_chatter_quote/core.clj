(ns clojure-chatter-quote.core
  (:require [clojure-chatter-quote.engine :as engine]
            [clojure-chatter-quote.database :as database]
            [clojure.string :refer [split-lines]])
  (:import java.io.File)
  (:gen-class ))

(defn quotes-from-file
  "Obtains all the quotes from the given file"
  [file]
  (->> file slurp split-lines))

;(defn read-config
;  "Read config from config.edn"
;  []
;  (edn/read-string (slurp "config.edn")))

(defn -main
  [& args]
  (println "Creating database and starting quote engine")
  (database/init)
  (engine/start)
  (println "Adding quotes from quote file")
  (doseq [quote (quotes-from-file (File. (System/getProperty "user.home") "quotes.txt"))]
    (database/add-quote quote "initial-group"))
  (println "Adding schedule to send entered quotes")
  (database/add-schedule "initial-group-schedule" "0 0 08-21/3 * * ?" ["initial-group"])
  (println "All done with initial setup; scheduler takes over from here.")
;  (database/add-quote "Test quote 1" ["tag1"])
;  (database/add-quote "Test quote 2" ["tag2"])
;  (database/add-schedule "schedule 1" "15,45 * * * * ?" "tag1")
;  (database/add-schedule "schedule 2" "0,31 * * * * ?" "tag2")
  )
