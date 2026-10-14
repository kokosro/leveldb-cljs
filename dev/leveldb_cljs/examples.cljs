(ns leveldb-cljs.examples
  "The examples of the README, as functions.

   `main` is the quick start: it opens a database in a fresh directory under
   the temp dir of the system, writes, reads and batches, closes and opens
   again, prints what it read and removes the directory. No network:

     npx shadow-cljs compile examples && node target/examples.js

   Every other function takes an open client, so it runs on a database on
   disk and on one in memory alike.

   The code between the README:begin / README:end marks is the code the
   README shows; test/leveldb_cljs/examples_test.cljs runs it and holds the
   two together."
  (:require ["fs" :as fs]
            ["os" :as os]
            ["path" :as path]
            [cljs.core.async :as async]
            [leveldb-cljs.core :as db]
            [leveldb-cljs.iter :as iter]
            [promesa.core :as p]))

;; ---------------------------------------------------------------------------
;; Quick start
;; ---------------------------------------------------------------------------

;; README:begin quick-start
(defn temp-dir!
  "A fresh directory under the temp dir of the system."
  []
  (fs/mkdtempSync (path/join (os/tmpdir) "quickstart-")))

(defn quick-start
  "Promise of everything the quick start reads, as a map. The database
   lives in the directory `dir`."
  [dir]
  (p/let [c        (db/open! {:path dir :value-encoding :edn})
          _        (db/put! c "user:alice" {:name "Alice" :score 620})
          alice    (db/get! c "user:alice")
          nobody   (db/get! c "user:nobody")
          _        (db/batch! c [{:type :put :key "user:bob" :value {:name "Bob" :score 10}}
                                 {:type :put :key "user:carol" :value {:name "Carol" :score 75}}
                                 {:type :put :key "visit:1" :value {:user "bob"}}
                                 {:type :del :key "user:alice"}])
          present  (db/has-many! c ["user:alice" "user:bob"])
          all-keys (db/keys! c)
          users    (db/all! c {:gte "user:" :lt "user;"})
          last-one (db/all! c {:reverse true :limit 1})
          _        (db/close! c)
          c        (db/open! {:path dir :value-encoding :edn})
          reopened (db/get! c "user:carol")
          _        (db/close! c)]
    {:alice alice
     :nobody nobody
     :present present
     :all-keys all-keys
     :users users
     :last-one last-one
     :reopened reopened}))

(def printed
  "The keys of the quick start in the order `main` prints them."
  [:alice :nobody :present :all-keys :users :last-one :reopened])

(defn main [& _]
  (let [dir (temp-dir!)]
    (-> (quick-start dir)
        (p/then (fn [result]
                  (doseq [k printed]
                    (println k (pr-str (get result k))))))
        (p/catch (fn [e]
                   (println "failed:" (ex-message e) (pr-str (db/error-code e)))
                   (set! (.-exitCode js/process) 1)))
        (p/finally (fn [_ _]
                     (fs/rmSync dir #js {:recursive true :force true}))))))
;; README:end quick-start

;; ---------------------------------------------------------------------------
;; Records and an index, written together
;; ---------------------------------------------------------------------------

;; README:begin index
(defn stores
  "The sublevels of an order book kept in the database `c`: the orders by
   id, and an index of order ids by customer."
  [c]
  {:orders (db/sublevel c "orders")
   :by-customer (db/sublevel c "by-customer" {:value-encoding :utf8})})

(defn place-order!
  "Stores `order` and its index entry in one atomic batch: both are
   written or neither is."
  [c {:keys [orders by-customer]} {:keys [id customer] :as order}]
  (db/batch! c [{:type :put :sublevel orders :key id :value order}
                {:type :put :sublevel by-customer :key (str customer "/" id) :value id}]))

(defn orders-of!
  "Promise of the orders of `customer`, in the order of their ids."
  [{:keys [orders by-customer]} customer]
  (p/let [ids (db/values! by-customer {:gte (str customer "/")
                                       :lt (str customer "0")})]
    (db/get-many! orders ids)))
;; README:end index

;; ---------------------------------------------------------------------------
;; A range as a stream
;; ---------------------------------------------------------------------------

;; README:begin scan
(defn total!
  "Promise of the sum of :amount over every order, read `batch-size` at a
   time: the orders are never in memory as a whole."
  [{:keys [orders]} batch-size]
  (iter/reduce! (fn [sum {:keys [value]}] (+ sum (:amount value)))
                0
                (iter/entries! orders {:batch-size batch-size :buffer batch-size})))

(defn first-over!
  "Promise of the first order with an :amount above `limit`, nil when
   there is none. The scan ends there and the iterator is released."
  [{:keys [orders]} limit]
  (iter/reduce! (fn [_ {:keys [value]}]
                  (when (> (:amount value) limit)
                    (reduced value)))
                nil
                (iter/entries! orders {:batch-size 2 :buffer 2})))
;; README:end scan

;; ---------------------------------------------------------------------------
;; Follow the writes
;; ---------------------------------------------------------------------------

;; README:begin follow
(defn follow!
  "Hands every write to the database `c` to `handle`, as a map. Returns the
   channel: closing it ends the subscription, and it closes by itself when
   the database is closed."
  [c handle]
  (let [ch (db/events! c {:only #{:put :del :batch :clear} :buffer 64})]
    (async/go-loop []
      (when-some [event (async/<! ch)]
        (handle event)
        (recur)))
    ch))
;; README:end follow

;; ---------------------------------------------------------------------------
;; The features, once each
;; ---------------------------------------------------------------------------

(defn tour!
  "Promise of what the features of the README resolve to, as a map. `c` is
   an open and empty client with :edn values."
  [c]
  (p/let [;; README:begin tour
          _        (db/put! c "a" {:n 1})
          _        (db/put! c "b" {:n 2})
          both     (db/get-many! c ["a" "b" "zz"])
          there?   (db/has! c "a")
          as-text  (db/get! c "a" {:value-encoding :utf8})
          seen     (db/with-snapshot! c (fn [snapshot]
                                          (p/do (db/put! c "c" {:n 3})
                                                (db/keys! c {:snapshot snapshot}))))
          _        (db/with-batch! c (fn [batch]
                                       (db/batch-put! batch "d" {:n 4})
                                       (db/batch-del! batch "a")))
          now      (db/keys! c)
          tail     (db/all! c {:gt "b" :reverse true :limit 1})
          _        (db/clear! c {:gte "c"})
          left     (db/keys! c)
          open     (db/open-resources c)
          ;; README:end tour
          ]
    {:both both
     :there? there?
     :as-text as-text
     :seen seen
     :now now
     :tail tail
     :left left
     :open open}))

;; ---------------------------------------------------------------------------
;; Errors
;; ---------------------------------------------------------------------------

;; README:begin errors
(defn explain
  "What went wrong, as data."
  [e]
  (select-keys (db/error-data e) [:type :code :reason :op :arg]))

(defn read!
  "Promise of {:value v} for the value under `key`, or of {:failed data}.
   Never rejects."
  [c key]
  (-> (db/get! c key)
      (p/then (fn [v] {:value v}))
      (p/catch (fn [e] {:failed (explain e)}))))
;; README:end errors
