(ns clojure-chatter-quote.engine
  (:require [clojure-chatter-quote.database :as database]
            [clojure.string :refer [split-lines]]
            [clojurewerkz.quartzite.scheduler :as scheduler]
            [clojurewerkz.quartzite.jobs :as jobs]
            [clojurewerkz.quartzite.triggers :as triggers]
            [clojurewerkz.quartzite.schedule.cron :as cron]
            [clojurewerkz.quartzite.schedule.simple :as simple]
            [clojurewerkz.quartzite.matchers :as matchers]
            [clojure.edn :as edn]
            [clojure.set :as set]
            [clj-time.core :as time])
  (:import java.util.concurrent.TimeUnit
           [org.quartz Scheduler]
           [java.util UUID Date]
           [org.jivesoftware.smack Connection ConnectionConfiguration ConnectionConfiguration$SecurityMode MessageListener XMPPConnection]
           [java.io File]))

; How often, in seconds, the database->scheduler synchronization should happen as described in sync-jobs-with-database.
; Presently, this is only read at start time, so if changed, you need to stop and start the engine for it to take
; effect.
(def scheduler-sync-interval-seconds 15)

; gmail address of user to send to
(def to-user "thestewartfamily@gmail.com")

; login info for the user sending the chats
(def login-user "rds6235@gmail.com")
(def login-password "xabpouwcugecwowr")

; Selecting a quote to send happens at a regular interval. In order to make the sending random, the quote isn't sent
; immediately. Instead, it's scheduled for sending at some later time. These properties control the delay before that
; happens. The value of the static component is added to a random number between 0 and the random component to get
; the number of seconds to delay sending. I.e.:
; (+ static (rand-int random))
(def send-delay-static-component 0)
(def send-delay-random-component 600)

; I use three Quartz job/trigger groups to drive everything:
; - The "internal jobs" group is for internal jobs that drive the app. At the moment, there's just one, which periodically
; syncs the scheduled jobs with the database.
; - The "sending schedule" group is for running the repeating message-sending jobs. This is the group that syncs with
; the database to cause messages to be sent at regular intervals.
; - The "single quote" group is the transient, one-shot jobs that actually deliver a message. This group exists because
; I want the sending interval to have some randomness, so a "sending schedule" just determines a time window for a
; message to be sent and then creates a new job in this group. The job executes after some random delay and doesn't
; repeat.
(def quartz-group-for-internal-jobs "engine")
(def quartz-group-for-sending-schedule "schedule")
(def quartz-group-for-single-quote-send "quote")

; Tracking files keep track of which quotes from a given batch have been sent. Each time one is sent, a line
; identifying the quote is added to its tracking file. Once all quotes are sent, the file is wiped and we start over.
; This is how I send random quotes but ensure the entire batch of quotes is sent before repeating any. Because the data
; model allows for many, different batches of quotes to be going on concurrently, it can't be one, static tracking
; file. Instead, I use a static path and base name, then add in the "sending schedule" name for uniqueness. Thus each
; schedule is tracked independently.
(def quote-tracking-file-path "/tmp")
(def quote-tracking-file-name-base "sent-quotes-")

; The log file to write quotes to when sent.
(def log-file (File. (System/getProperty "user.home") "chatter-quote/quotes.log"))

(defn quote-tracking-file
  "Returns the tracking file that's keeping track of which quotes have been sent in
  a particular schedule.
  The argument is the schedule name."
  [schedule-name]
  (let [munged-name (-> schedule-name (.replaceAll " " "_") (.replaceAll "\\W" ""))
        quote-tracking-file-name (str quote-tracking-file-name-base munged-name ".txt")
        file (File. quote-tracking-file-path quote-tracking-file-name)]
    (spit file "" :append true) ; As close as I could think of to "touch" to ensure the file exists
    file))

(defn select-unsent-quote
  "Uses the tracking file to pick one of the provided quotes that hasn't been
  sent within the memory of the file. Does *not* update the tracking file to reflect
  the selected quote. The only change that may be made to the file by this function
  is that it may cycle the file by emptying it if all eligible quotes have already
  been sent.
  The first argument is the quotes to choose from.
  The second argument is the file being used to track this set of quotes."
  [eligible-quotes tracking-file]
  (let [sent-ids (->> tracking-file slurp split-lines (filter (comp not empty?)) (map #(Long/parseLong %)) set)
        eligible-ids (->> eligible-quotes (map :id) set)
        unsent-ids (set/difference eligible-ids sent-ids)]
    (if (empty? unsent-ids)
      (do
        (spit tracking-file "")
        (recur eligible-quotes tracking-file))
      (let [selected-id (rand-nth (vec unsent-ids))]
        ; a kinda weird, but fairly concise, way to find the first item in a collection
        ; that meets some criteria
        (some #(if (= (:id %) selected-id) %) eligible-quotes)))))

; A noop callback required by Smack when creating an XMPP chat
(def xmpp-noop-listener (reify MessageListener (processMessage [this chat message] ())))

(defn send-xmpp-message
  "Use my gtalk account to send a message. The arg is the string to send.
  There's a lot of stuff here that could be extracted and made generally reusable."
  [message]
  (try
    (def xmpp-config (doto
                       (ConnectionConfiguration. "talk.google.com", 5222, "gmail.com")
                       (.setCompressionEnabled true)
                       (.setSecurityMode ConnectionConfiguration$SecurityMode/required)))
    (def xmpp-connection (doto
                           (XMPPConnection. xmpp-config)
                           (.connect)
                           (.login login-user login-password, "chatter-quote")))
    (def chat (-> xmpp-connection .getChatManager (.createChat to-user, xmpp-noop-listener)))
    (.sendMessage chat (str "Say:\n" message))
    (finally (.disconnect xmpp-connection)))
  (spit log-file (format "%tFT%<tT %s%n" (Date.) message) :append true))

; Quartz job that fires off an XMPP message. The message text is looked up in the job data map as "text".
(jobs/defjob send-xmpp-message-job [ctx]
  (try
    (send-xmpp-message (get (.getMergedJobDataMap ctx) "text"))
    (catch Exception e (.printStackTrace e))))

(defn select-and-schedule-quote-for-sending
  "Do all the work of picking a quote and setting it up to be sent. Sending is
  done after a delay by scheduling a new Quartz job, so this function does pretty
  much everything except actually send.
  The first argument is a list of tags from which the quote should be selected.
  Second argument is the file to use for tracking this set of quotes."
  [tags quote-tracking-file]
  (let [eligible-quotes (database/quotes-with-tag tags)]
    (if (empty? eligible-quotes)
      (println "No eligible quotes for tags: " tags)
      (let [quote-to-send (select-unsent-quote eligible-quotes quote-tracking-file)]
        (scheduler/schedule
          (jobs/build
            (jobs/with-identity (str (UUID/randomUUID)) quartz-group-for-single-quote-send)
            (jobs/of-type send-xmpp-message-job)
            (jobs/using-job-data {:text (:text quote-to-send)}))
          (triggers/build
            (triggers/with-identity (str (UUID/randomUUID)) quartz-group-for-single-quote-send)
            (triggers/start-at (time/plus (time/now) (time/secs (+ send-delay-static-component (rand-int send-delay-random-component)))))
            (triggers/with-schedule (simple/schedule (simple/with-repeat-count 0)))))
        ; Note to self: using the db id to track the quotes works mostly, but not across restarts. Would be
        ; better to hash the quote and store that, maybe. That would also prevent accidental duplicates
        ; from slipping in.
        (spit quote-tracking-file (format "%s%n" (:id quote-to-send)) :append true)))))

; Quartz job that does the main work: calls the function that picks a quote and gets it ready to send.
(jobs/defjob select-and-schedule-quote-for-sending-job [ctx]
  (try
    (select-and-schedule-quote-for-sending
      (get (.getMergedJobDataMap ctx) "tags")
      (get (.getMergedJobDataMap ctx) "tracking-file"))
    (catch Exception e (.printStackTrace e))))

(defn create-schedule-if-missing
  "Adds a sending schedule to the scheduler if it doesn't yet exist.
  A sending schedule is something that has :name, :cron, and :tags keys, which
  indicate the unique schedule name, the cron schedule to fire on, and
  the quote tags that should be considered for sending on this schedule,
  respectively.
  Existence is determined by the name."
  [{:keys [name cron tags]}]
  (let [scheduled
        (scheduler/maybe-schedule
          (jobs/build
            (jobs/of-type select-and-schedule-quote-for-sending-job)
            (jobs/with-identity name quartz-group-for-sending-schedule)
            (jobs/using-job-data {:tags tags :tracking-file (quote-tracking-file name)}))
          (triggers/build
            triggers/start-now
            (triggers/with-identity name quartz-group-for-sending-schedule)
            (triggers/with-schedule (cron/cron-schedule cron))))]
    (if scheduled (println "Added job to scheduler for new sending schedule" name))))

(defn update-schedule-if-exists
  "If a sending schedule already exists in the scheduler, ensures its
  configuration is up to date with what's specified. The schedule
  and existence are defined as in create-schedule-if-missing.

  NOTE: At present, the 'if exists' part doesn't happen. The schedule
  must exist!"
  [{:keys [name cron tags]}]
  (let [existing-job (scheduler/get-job name quartz-group-for-sending-schedule)
        existing-trigger (scheduler/get-trigger name quartz-group-for-sending-schedule)
        existing-cron-expression (.getCronExpression existing-trigger )
        existing-tags (get (.getJobDataMap existing-job) "tags")]
    (if (not (and
               (= cron existing-cron-expression)
               (= tags existing-tags)))
      (do
        (scheduler/delete-job (.getKey existing-job))
        (scheduler/schedule
          (jobs/build
            (jobs/of-type select-and-schedule-quote-for-sending-job)
            (jobs/with-identity name quartz-group-for-sending-schedule)
            (jobs/using-job-data {:tags tags}))
          (triggers/build
            triggers/start-now
            (triggers/with-identity name quartz-group-for-sending-schedule)
            (triggers/with-schedule (cron/cron-schedule cron))))
        (println "Updated schedule" name "to match database.")))))

(defn remove-obsolete-schedules
  "Removes any sending schedules from the scheduler whose names aren't in
  the provided list. Note this is a simple list of names, not a list
  of schedules."
  [schedule-names]
  (doseq [existing-job (scheduler/get-matching-jobs (matchers/group-equals quartz-group-for-sending-schedule))]
    (let [name (->> existing-job .getKey .getName)]
      ; Reminder: contains? isn't "contains" in Clojure! Have to do this crazy "some" thing.
      (if (not (some #{name} schedule-names))
        (do
          (scheduler/delete-job (jobs/key name quartz-group-for-sending-schedule))
          (println "Unscheduled" name "because it's no longer in the database."))))))

(defn sync-jobs-with-database
  "Loads all sending schedules from the database and ensures the scheduler is up to date
  with what the database says should be there."
  []
  (let [schedules (database/all-schedules)]
    (doseq [schedule schedules]
      (create-schedule-if-missing schedule)
      (update-schedule-if-exists schedule))
    (remove-obsolete-schedules (vec (map :name schedules)))))

; Quartz job that kicks of the function that syncs the Quartz scheduler to what's in the database
(jobs/defjob sync-scheduler-with-database-job [ctx]
  (try
    (sync-jobs-with-database)
    (catch Exception e (.printStackTrace e))))

(defn start
  "Starts the engine, so to speak."
  []
  (scheduler/initialize)
  (scheduler/start)
  (scheduler/schedule
    (jobs/build
      (jobs/with-identity "Sync quote schedules with database" "engine")
      (jobs/of-type sync-scheduler-with-database-job))
    (triggers/build
      (triggers/with-identity "Sync quote schedules with database" "engine")
      triggers/start-now
      (triggers/with-schedule
        (simple/schedule (simple/with-interval-in-seconds scheduler-sync-interval-seconds) (simple/repeat-forever))))))

(defn stop
  "Shuts everything down - opposite of start."
  []
  (scheduler/shutdown))
