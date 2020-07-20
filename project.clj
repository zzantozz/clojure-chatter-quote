(defproject clojure-chatter-quote "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-http "0.4.1"]
                 [com.datomic/datomic-free "0.8.4020.24"]
                 [clojurewerkz/quartzite "1.1.0"]
                 [clj-time "0.5.1"]
                 [org.igniterealtime.smack/smack "3.2.1"]

                 [org.clojure/data.xml "0.0.7"]

                 [org.igniterealtime.smack/smackx "3.2.1"]]
  :main clojure-chatter-quote.core)
