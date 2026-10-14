(ns leveldb-cljs.core-test
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.string :as str]
            [promesa.core :as p]
            [leveldb-cljs.core :as core]
            [leveldb-cljs.test-util :as tu]
            [leveldb-cljs.util :as util]))

(defn- db-error? [e] (= :leveldb-cljs/db-error (util/error-type e)))
(defn- argument-error? [e] (= :leveldb-cljs/argument-error (util/error-type e)))
(defn- codec-error? [e] (= :leveldb-cljs/codec-error (util/error-type e)))

;; ---------------------------------------------------------------------------
;; lifecycle
;; ---------------------------------------------------------------------------

(deftest open-memory-client
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (is (core/client? c))
          (is (core/root? c))
          (is (= :open (core/status c)))
          (is (core/open? c))
          (is (= :memory (core/backend c)))
          (is (nil? (core/location c)))
          (is (nil? (core/parent c)))
          (is (identical? c (core/root c)))
          (is (= "" (core/prefix c)))
          (is (= [] (core/path c)))
          (is (= {:name :utf8 :format :utf8 :upstream "utf8"} (core/key-encoding c)))
          (is (= {:name :utf8 :format :utf8 :upstream "utf8"} (core/value-encoding c)))
          (let [s (core/supports c)]
            (is (map? s))
            (is (false? (:permanence s)))
            (is (true? (:implicit-snapshots s)) "manifest keys are kebab keywords")
            (is (true? (:explicit-snapshots s)))
            (is (true? (:get-sync s)))
            (is (true? (get-in s [:encodings :utf8])))
            (is (= {} (:additional-methods s)))))))))

(deftest open-temp-dir-client
  (async done
    (tu/finish done
      (tu/with-tmp-db
        (fn [c]
          (is (= :level (core/backend c)))
          (is (string? (core/location c)))
          (is (str/ends-with? (core/location c) "/db"))
          (let [files (tu/dir-files (core/location c))]
            (is (some #{"CURRENT"} files) "a LevelDB directory was created")
            (is (some #{"LOCK"} files)))
          (let [s (core/supports c)]
            (is (true? (:permanence s)))
            (is (true? (:create-if-missing s)))
            (is (true? (get-in s [:additional-methods :approximate-size])))))))))

(deftest open-with-string-path-and-passthrough-options
  (async done
    (tu/finish done
      (tu/with-tmp-dir
        (fn [dir]
          (p/let [c (core/open! (str dir "/one"))
                  _ (core/put! c "k" "v")
                  v (core/get! c "k")
                  _ (core/close! c)
                  ;; kebab -> camel passthrough of classic-level tuning options
                  c2 (core/open! {:path (str dir "/two") :compression false :cache-size 1024
                                  :write-buffer-size 65536 :max-open-files 50
                                  :create-if-missing? true})
                  _ (core/put! c2 "k" "v")
                  v2 (core/get! c2 "k")
                  _ (core/close! c2)]
            (is (= "v" v))
            (is (= "v" v2))))))))

(deftest open-rejects-bad-arguments-before-upstream
  (async done
    (tu/finish done
      (p/let [e1 (tu/rejection (core/open! 42))
              e2 (tu/rejection (core/open! {:memory? true :bogus 1}))
              e3 (tu/rejection (core/open! {}))
              e4 (tu/rejection (core/open! {:memory? true :value-encoding :transit}))
              e5 (tu/rejection (core/open! {:memory? true :create-if-missing? "yes" :nope 2}))]
        (is (argument-error? e1))
        (is (argument-error? e2))
        (is (= #{:bogus} (:unknown (ex-data e2))))
        (is (argument-error? e3) "level backend needs a path")
        (is (= :path (:arg (ex-data e3))))
        (is (argument-error? e4))
        (is (= :codec (:arg (ex-data e4))))
        (is (argument-error? e5) "unknown key reported even with other options present")))))

(deftest double-open-is-idempotent-and-reopen-works
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [again (core/open! c)
                  _ (core/put! c "k" "v")
                  _ (core/close! c)
                  st (core/status c)
                  _ (core/close! c)
                  reopened (core/open! c)
                  v (core/get! c "k")]
            (is (identical? c again) "open! on an open client resolves the same client")
            (is (= :closed st))
            (is (identical? c reopened))
            (is (= :open (core/status c)))
            (is (= "v" v) "data survives close + reopen")))))))

(deftest close-then-op-rejects-with-not-open-taxonomy
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (core/close! c)
                  e-get (tu/rejection (core/get! c "k"))
                  e-put (tu/rejection (core/put! c "k" "v"))
                  e-del (tu/rejection (core/del! c "k"))
                  e-has (tu/rejection (core/has! c "k"))
                  e-batch (tu/rejection (core/batch! c [{:type :put :key "k" :value "v"}]))
                  e-clear (tu/rejection (core/clear! c))
                  e-all (tu/rejection (core/all! c))
                  e-it (tu/thrown #(core/iterator c))
                  e-cb (tu/thrown #(core/chained-batch c))
                  e-sync (tu/thrown #(core/get-sync c "k"))]
            (is (= :closed (core/status c)))
            (doseq [[label e] {:get e-get :put e-put :del e-del :has e-has :batch e-batch
                               :clear e-clear :all e-all :iterator e-it :chained-batch e-cb
                               :get-sync e-sync}]
              (is (db-error? e) (str label " is a db-error"))
              (is (= :database-not-open (util/error-code e)) (str label " code"))
              (is (util/not-open? e) (str label " not-open?"))
              (is (some? (:cause (ex-data e))) (str label " keeps the upstream error as :cause")))
            (is (= :get (:op (ex-data e-get))))
            (is (= :iterator (:op (ex-data e-it))))
            (is (= "Error" (:name (ex-data e-get))) "ModuleError's name is 'Error' (inventory 3.1)")
            ;; the fixture closes again: close! must be idempotent
            (core/close! c)))))))

(deftest lock-failure-surfaces-cause-chain
  (async done
    (tu/finish done
      (tu/with-tmp-db
        (fn [c]
          (p/let [e (tu/rejection (core/open! (core/location c)))]
            (is (db-error? e))
            (is (= :database-not-open (util/error-code e)))
            (is (= :locked (:cause-code (ex-data e))) "cause.code LEVEL_LOCKED -> :locked")
            (is (= :locked (:reason (ex-data e))))
            (is (str/includes? (:cause-message (ex-data e)) "LOCK"))
            (is (= :open (:op (ex-data e))))
            (is (some? (:cause (ex-data e))))
            (is (= :open (core/status c)) "the first client is unaffected")))))))

(deftest create-if-missing-and-error-if-exists-reasons
  (async done
    (tu/finish done
      (tu/with-tmp-dir
        (fn [dir]
          (p/let [e-missing (tu/rejection (core/open! {:path (str dir "/missing") :create-if-missing? false}))
                  c (core/open! (str dir "/exists"))
                  _ (core/close! c)
                  e-exists (tu/rejection (core/open! {:path (str dir "/exists") :error-if-exists? true}))]
            (is (= :database-not-open (util/error-code e-missing)))
            (is (= :missing (:reason (ex-data e-missing))))
            (is (nil? (:cause-code (ex-data e-missing))) "message-only cause (inventory 3.4)")
            (is (str/includes? (:cause-message (ex-data e-missing)) "does not exist"))
            (is (= :database-not-open (util/error-code e-exists)))
            (is (= :exists (:reason (ex-data e-exists))))
            (is (str/includes? (:cause-message (ex-data e-exists)) "exists"))))))))

;; ---------------------------------------------------------------------------
;; reads and writes
;; ---------------------------------------------------------------------------

(deftest put-get-del-has-on-both-backends
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c backend]
          (p/let [r-put (core/put! c "alice" "1")
                  v (core/get! c "alice")
                  h (core/has! c "alice")
                  h2 (core/has! c "nobody")
                  r-del (core/del! c "alice")
                  v2 (core/get! c "alice")
                  r-del2 (core/del! c "alice")
                  _ (core/put! c "" "empty-key")
                  ek (core/get! c "")
                  _ (core/put! c "k" "")
                  ev (core/get! c "k")]
            (is (nil? r-put) (str backend " put! resolves nil"))
            (is (= "1" v))
            (is (true? h))
            (is (false? h2))
            (is (nil? r-del))
            (is (nil? v2) "deleted key reads nil")
            (is (nil? r-del2) "deleting a missing key resolves")
            (is (= "empty-key" ek) "zero-length keys are valid")
            (is (= "" ev) "zero-length values are valid")))))))

(deftest get-of-missing-key-is-nil-not-an-error
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [v (core/get! c "missing")
                  vs (core/get-many! c ["missing" "also"])
                  s (core/get-sync c "missing")]
            (is (nil? v))
            (is (= [nil nil] vs))
            (is (nil? s))))))))

(deftest get-sync-reads-synchronously
  (async done
    (tu/finish done
      (tu/with-each-backend
        {:value-encoding :edn}
        (fn [c _]
          (p/let [_ (core/put! c "k" {:n 1})]
            (is (= {:n 1} (core/get-sync c "k")))
            (is (= "{:n 1}" (core/get-sync c "k" {:value-encoding :utf8})) "per-call override")
            (is (nil? (core/get-sync c "zzz")))
            (is (argument-error? (tu/thrown #(core/get-sync c nil))))
            (is (argument-error? (tu/thrown #(core/get-sync c "k" {:nope 1}))))))))))

(deftest get-many-and-has-many
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (tu/seed! c {"a" "1" "b" "2"})
                  vs (core/get-many! c ["a" "zz" "b"])
                  hs (core/has-many! c ["a" "zz" "b"])
                  none (core/get-many! c [])
                  e (tu/rejection (core/get-many! c "a"))
                  e2 (tu/rejection (core/has-many! c ["a" nil]))]
            (is (= ["1" nil "2"] vs))
            (is (= [true false true] hs))
            (is (= [] none))
            (is (argument-error? e))
            (is (argument-error? e2) "nil inside the key vector")))))))

(deftest nil-key-or-value-is-an-argument-error-before-upstream
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [e1 (tu/rejection (core/put! c nil "v"))
                  e2 (tu/rejection (core/put! c "k" nil))
                  e3 (tu/rejection (core/get! c nil))
                  e4 (tu/rejection (core/del! c nil))
                  e5 (tu/rejection (core/has! c nil))
                  e6 (tu/rejection (core/get! c "k" {:snapshot "nope"}))
                  e7 (tu/rejection (core/put! c "k" "v" {:value-encodings :edn}))
                  ks (core/keys! c)]
            (is (argument-error? e1))
            (is (= :key (:arg (ex-data e1))))
            (is (argument-error? e2))
            (is (= :value (:arg (ex-data e2))))
            (is (argument-error? e3))
            (is (argument-error? e4))
            (is (argument-error? e5))
            (is (argument-error? e6) ":snapshot must be a handle")
            (is (argument-error? e7) "typo in an option key is caught (pitfall 8.17)")
            (is (= #{:value-encodings} (:unknown (ex-data e7))))
            (is (= [] ks) "nothing reached the database")))))))

;; ---------------------------------------------------------------------------
;; codecs through the database
;; ---------------------------------------------------------------------------

(deftest utf8-never-stringifies-silently
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [e1 (tu/rejection (core/put! c "m" {:a 1}))
                  e2 (tu/rejection (core/put! c "n" 42))
                  e3 (tu/rejection (core/put! c {:key 1} "v"))
                  e4 (tu/rejection (core/batch! c [{:type :put :key "b" :value [1 2]}]))
                  ks (core/keys! c)]
            (is (codec-error? e1))
            (is (= :utf8 (:codec (ex-data e1))))
            (is (= :value (:role (ex-data e1))))
            (is (codec-error? e2))
            (is (codec-error? e3))
            (is (= :key (:role (ex-data e3))))
            (is (codec-error? e4) "batch ops go through the codec layer too")
            (is (= [] ks) "nothing was stored as \"[object Object]\"")))))))

(deftest edn-json-default-and-per-call-codecs
  (async done
    (tu/finish done
      (tu/with-each-backend
        {:value-encoding :edn}
        (fn [c _]
          (let [data {:id 7 :tags #{:a :b} :nested {"s" [1 2 3]} :kw :x}]
            (p/let [_ (core/put! c "edn" data)
                    back (core/get! c "edn")
                    raw (core/get! c "edn" {:value-encoding :utf8})
                    _ (core/put! c "json" {:a [1 {:b nil}]} {:value-encoding :json})
                    j (core/get! c "json" {:value-encoding :json})
                    jraw (core/get! c "json" {:value-encoding :utf8})
                    e (tu/rejection (core/get! c "edn" {:value-encoding :json}))
                    _ (core/put! c "s" "{plain text" {:value-encoding :utf8})
                    e2 (tu/rejection (core/get! c "s"))]
              (is (= data back) "exact EDN round-trip")
              (is (= (pr-str data) raw) "stored as EDN text")
              (is (= {:a [1 {:b nil}]} j))
              (is (= "{\"a\":[1,{\"b\":null}]}" jraw) "stored as JSON text")
              (is (codec-error? e) "EDN text is not JSON: decode error is a codec-error")
              (is (= :decode (:direction (ex-data e))))
              (is (codec-error? e2) "unbalanced text is not EDN")
              (is (= "{plain text" (:value (ex-data e2))) "the raw value is reported"))))))))

(deftest json-and-edn-keys
  (async done
    (tu/finish done
      (tu/with-memory
        {:key-encoding :edn :value-encoding :edn}
        (fn [c]
          (p/let [_ (core/put! c [:user 1] {:n "one"})
                  _ (core/put! c [:user 2] {:n "two"})
                  _ (core/put! c {:m 1} :map-key {:key-encoding :json})
                  v (core/get! c [:user 2])
                  j (core/get! c {:m 1} {:key-encoding :json})
                  ks (core/keys! c {:gte [:user 0] :lte [:user 9]})
                  raw (core/keys! c {:key-encoding :utf8})]
            (is (= {:n "two"} v))
            (is (= :map-key j))
            (is (= [[:user 1] [:user 2]] ks) "range bounds are encoded with the key codec")
            (is (= ["[:user 1]" "[:user 2]" "{\"m\":1}"] raw))))))))

(deftest hex-base64-and-bytes-round-trip
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (core/put! c "h" "deadbeef" {:value-encoding :hex})
                  h (core/get! c "h" {:value-encoding :hex})
                  b (core/get! c "h" {:value-encoding :buffer})
                  raw (core/get! c "h" {:value-encoding :raw})
                  b64 (core/get! c "h" {:value-encoding :base64})
                  _ (core/put! c "b" (core/base64->bytes "aGVsbG8=") {:value-encoding :buffer})
                  s (core/get! c "b")
                  v (core/get! c "b" {:value-encoding :view})
                  _ (core/put! c "r" "raw text" {:value-encoding :raw})
                  r (core/get! c "r")
                  rb (core/get! c "r" {:value-encoding :raw})
                  e (tu/rejection (core/put! c "x" "xyz" {:value-encoding :hex}))
                  e2 (tu/rejection (core/put! c "x" "text" {:value-encoding :buffer}))]
            (is (= "deadbeef" h))
            (is (core/bytes? b))
            (is (= 4 (core/bytes-length b)) "hex was stored as 4 bytes, not 8 characters")
            (is (= "deadbeef" (core/bytes->hex b)))
            (is (core/bytes= b raw))
            (is (= "3q2+7w==" b64))
            (is (= "hello" s) "bytes written are readable as utf8 text")
            (is (core/bytes? v))
            (is (= "hello" (core/bytes->utf8 v)))
            (is (= "raw text" r))
            (is (= "raw text" (core/bytes->utf8 rb)))
            (is (codec-error? e) "odd hex string is rejected before upstream")
            (is (codec-error? e2) ":buffer does not accept strings")))))))

(deftest bytes-helpers
  (let [b (core/utf8->bytes "héllo")]
    (is (core/bytes? b))
    (is (= 6 (core/bytes-length b)))
    (is (= "héllo" (core/bytes->utf8 b)))
    (is (= "68c3a96c6c6f" (core/bytes->hex b)))
    (is (= "aMOpbGxv" (core/bytes->base64 b)))
    (is (core/bytes= b (core/hex->bytes "68c3a96c6c6f")))
    (is (core/bytes= b (core/base64->bytes "aMOpbGxv")))
    (is (not (core/bytes= b (core/utf8->bytes "hello"))))
    (is (argument-error? (tu/thrown #(core/bytes->hex "nope"))))
    (is (argument-error? (tu/thrown #(core/hex->bytes "abc"))))
    (is (argument-error? (tu/thrown #(core/base64->bytes "a$"))))
    (is (argument-error? (tu/thrown #(core/utf8->bytes 1))))
    (is (argument-error? (tu/thrown #(core/bytes-length nil))))))

;; ---------------------------------------------------------------------------
;; batch
;; ---------------------------------------------------------------------------

(deftest batch-array-form
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (tu/seed! c {"a" "1" "b" "2" "c" "3"})
                  r (core/batch! c [{:type :put :key "d" :value "4"}
                                    {:type :del :key "a"}
                                    {:type :put :key "b" :value "22"}
                                    {:type :del :key "never-there"}])
                  all (core/all! c)
                  _ (core/batch! c [])
                  _ (core/batch! c [{:type :put :key "e" :value "{:x 1}" :value-encoding :utf8}
                                    {:type :put :key "f" :value {:x 2} :value-encoding :edn}]
                                 {:sync true})
                  ef (core/get-many! c ["e" "f"] {:value-encoding :edn})]
            (is (nil? r))
            (is (= [{:key "b" :value "22"} {:key "c" :value "3"} {:key "d" :value "4"}] all))
            (is (= [{:x 1} {:x 2}] ef) "per-op :value-encoding honoured")))))))

(deftest batch-validation-happens-before-any-write
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [e1 (tu/rejection (core/batch! c [{:type :put :key "ok" :value "1"}
                                                  {:type :upsert :key "x" :value "1"}]))
                  e2 (tu/rejection (core/batch! c [{:type :put :value "1"}]))
                  e3 (tu/rejection (core/batch! c [{:type :put :key "k"}]))
                  e4 (tu/rejection (core/batch! c [{:type :del :key "k" :extra 1}]))
                  e5 (tu/rejection (core/batch! c {:type :put}))
                  e6 (tu/rejection (core/batch! c ["not a map"]))
                  e7 (tu/rejection (core/batch! c [{:type :put :key "k" :value "v"}] {:sync true :oops 1}))
                  ks (core/keys! c)]
            (is (argument-error? e1))
            (is (= 1 (:index (ex-data e1))))
            (is (argument-error? e2))
            (is (argument-error? e3))
            (is (argument-error? e4))
            (is (argument-error? e5))
            (is (argument-error? e6))
            (is (argument-error? e7))
            (is (= [] ks) "the valid op of the failed batch was not written")))))))

(deftest batch-with-sublevel-ops-and-root-identity
  (async done
    (tu/finish done
      (tu/with-memory
        {:value-encoding :edn}
        (fn [c]
          (p/let [people (core/sublevel c "people")
                  nested (core/sublevel people "nested" {:value-encoding :json})
                  _ (core/batch! c [{:type :put :key "root" :value {:r 1}}
                                    {:type :put :key "ann" :value {:age 3} :sublevel people}
                                    {:type :put :key "deep" :value {:d true} :sublevel nested}])
                  ann (core/get! people "ann")
                  deep (core/get! nested "deep")
                  root-keys (core/keys! c)
                  deep-raw (core/get! c "!people!!nested!deep" {:value-encoding :utf8})
                  _ (core/batch! people [{:type :del :key "ann"}
                                         {:type :put :key "bob" :value {:age 4}}
                                         {:type :put :key "sib" :value {:s 1} :sublevel nested}])
                  people-keys (core/keys! people)
                  nested-keys (core/keys! nested)
                  b (core/chained-batch c)
                  _ (core/batch-put! b "cb" {:via :chained} {:sublevel people})
                  _ (core/batch-write! b)
                  cb (core/get! people "cb")]
            (is (= {:age 3} ann))
            (is (= {:d true} deep))
            (is (= ["!people!!nested!deep" "!people!ann" "root"] root-keys) "keys are prefixed")
            (is (= "{\"d\":true}" deep-raw) "encoded with the SUBLEVEL's codec (json)")
            (is (= ["!nested!deep" "!nested!sib" "bob"] people-keys)
                "a parent sublevel sees its nested sublevel's prefixed keys")
            (is (= ["deep" "sib"] nested-keys) "a sublevel batch may target its descendants")
            (is (= {:via :chained} cb))
            (tu/with-memory
              (fn [other]
                (p/let [foreign (core/sublevel other "people")
                        e (tu/rejection (core/batch! c [{:type :put :key "lost" :value {:x 1} :sublevel foreign}]))
                        e2 (tu/rejection (core/batch! c [{:type :put :key "k" :value {:x 1} :sublevel other}]))
                        e3 (tu/thrown #(core/batch-put! (core/chained-batch c) "k" {:x 1} {:sublevel foreign}))
                        has-lost (core/has! c "!people!lost")
                        has-lost2 (core/has! foreign "lost")]
                  (is (argument-error? e) "foreign-root sublevel rejected (inventory 9.2)")
                  (is (= :sublevel (:arg (ex-data e))))
                  (is (argument-error? e2) "a root client is not a sublevel")
                  (is (argument-error? e3) "chained batches validate the same way")
                  (is (false? has-lost))
                  (is (false? has-lost2) "nothing vanished silently anywhere"))))))))))

(deftest chained-batch-builder
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (tu/seed! c {"a" "1" "b" "2"})
                  b (core/chained-batch c)
                  _ (is (= 0 (core/batch-length b)))
                  _ (-> b
                        (core/batch-put! "c" "3")
                        (core/batch-del! "a")
                        (core/batch-put! "d" "{:x 1}" {:value-encoding :utf8}))
                  len (core/batch-length b)
                  before (core/keys! c)
                  _ (core/batch-write! b)
                  after (core/all! c)
                  e-after (tu/thrown #(core/batch-put! b "e" "5"))
                  e-nil (tu/thrown #(core/batch-put! (core/chained-batch c) nil "5"))
                  e-codec (tu/thrown #(core/batch-put! (core/chained-batch c) "k" {:not "utf8"}))
                  e-opts (tu/thrown #(core/batch-put! (core/chained-batch c) "k" "v" {:sync true}))
                  b2 (core/chained-batch c)
                  _ (core/batch-put! b2 "x" "9")
                  _ (core/batch-clear! b2)
                  len2 (core/batch-length b2)
                  _ (core/batch-put! b2 "y" "9")
                  _ (core/batch-close! b2)
                  _ (core/batch-close! b2)
                  y (core/get! c "y")]
            (is (= 3 len))
            (is (= ["a" "b"] before) "nothing written before write!")
            (is (= [{:key "b" :value "2"} {:key "c" :value "3"} {:key "d" :value "{:x 1}"}] after))
            (is (db-error? e-after))
            (is (= :batch-not-open (util/error-code e-after)))
            (is (util/not-open? e-after))
            (is (argument-error? e-nil))
            (is (codec-error? e-codec))
            (is (argument-error? e-opts))
            (is (= 0 len2) "batch-clear! drops queued ops")
            (is (nil? y) "batch-close! discards")))))))

(deftest with-batch-writes-or-discards
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [r (core/with-batch! c (fn [b] (core/batch-put! b "a" "1") (core/batch-put! b "b" "2")))
                  ks (core/keys! c)
                  e (tu/rejection (core/with-batch! c (fn [b]
                                                        (core/batch-put! b "c" "3")
                                                        (throw (ex-info "abort" {:mine true})))))
                  ks2 (core/keys! c)
                  e2 (tu/rejection (core/with-batch! c (fn [b] (core/batch-put! b "d" {:bad 1}))))
                  res (core/open-resources c)]
            (is (nil? r))
            (is (= ["a" "b"] ks))
            (is (= "abort" (ex-message e)) "the user's error is rethrown")
            (is (= ["a" "b"] ks2) "the batch was discarded")
            (is (codec-error? e2))
            (is (= 0 (:batches res)) "every batch handle was released")))))))

;; ---------------------------------------------------------------------------
;; sublevels
;; ---------------------------------------------------------------------------

(deftest sublevel-isolation-and-linkage
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [people (core/sublevel c "people")
                  nested (core/sublevel people "nested")
                  arr (core/sublevel c ["people" "nested"])
                  hash (core/sublevel c "sep" {:separator "#"})
                  _ (core/put! c "ann" "root")
                  _ (core/put! people "ann" "people")
                  _ (core/put! nested "ann" "nested")
                  _ (core/put! hash "ann" "hash")
                  _ (core/put! people "x!y" "separator inside a key")
                  root-keys (core/keys! c)
                  people-all (core/all! people)
                  nested-keys (core/keys! nested)
                  arr-v (core/get! arr "ann")
                  through-root (core/get! c "!people!ann")
                  ranged (core/keys! people {:gt "b"})
                  cleared (core/clear! people)
                  after-clear (core/keys! c)]
            (is (= "!people!" (core/prefix people)))
            (is (= "!people!!nested!" (core/prefix nested)))
            (is (= "#sep#" (core/prefix hash)))
            (is (= ["people"] (core/path people)))
            (is (= ["people" "nested"] (core/path nested)))
            (is (= ["nested"] (core/path nested true)))
            (is (identical? c (core/parent people)))
            (is (identical? people (core/parent nested)))
            (is (identical? c (core/parent arr)) "array form: the db is the direct parent")
            (is (identical? c (core/root nested)))
            (is (not (core/root? people)))
            (is (= ["!people!!nested!ann" "!people!ann" "!people!x!y" "#sep#ann" "ann"] root-keys))
            (is (= [{:key "!nested!ann" :value "nested"} {:key "ann" :value "people"}
                    {:key "x!y" :value "separator inside a key"}]
                   people-all)
                "the sublevel sees only keys under its prefix, unprefixed (nested ones included)")
            (is (= ["ann"] nested-keys))
            (is (= "nested" arr-v) "array form addresses the same prefix")
            (is (= "people" through-root))
            (is (= ["x!y"] ranged) "range options are prefixed")
            (is (nil? cleared))
            (is (= ["#sep#ann" "ann"] after-clear) "clear! on a sublevel is scoped to it")))))))

(deftest sublevel-codecs-and-validation
  (async done
    (tu/finish done
      (tu/with-memory
        {:value-encoding :edn}
        (fn [c]
          (p/let [inherit (core/sublevel c "inherit")
                  json (core/sublevel c "json" {:value-encoding :json :key-encoding :utf8})
                  _ (core/put! inherit "k" {:edn true})
                  _ (core/put! json "k" {:json true})
                  i (core/get! inherit "k")
                  j (core/get! json "k")
                  raw (core/get-many! c ["!inherit!k" "!json!k"] {:value-encoding :utf8})
                  e-name (tu/thrown #(core/sublevel c 42))
                  e-empty (tu/thrown #(core/sublevel c []))
                  e-prefix (tu/thrown #(core/sublevel c "héllo"))
                  e-sep (tu/thrown #(core/sublevel c "a!b"))
                  e-opts (tu/thrown #(core/sublevel c "ok" {:bogus 1}))
                  e-codec (tu/rejection (core/put! json "k" (fn [])))]
            (is (= {:name :edn :format :utf8 :upstream "utf8"} (core/value-encoding inherit))
                "sublevels inherit the parent's codecs")
            (is (= {:name :json :format :utf8 :upstream "utf8"} (core/value-encoding json)))
            (is (= {:edn true} i))
            (is (= {:json true} j))
            (is (= ["{:edn true}" "{\"json\":true}"] raw))
            (is (argument-error? e-name))
            (is (argument-error? e-empty))
            (is (db-error? e-prefix))
            (is (= :invalid-prefix (util/error-code e-prefix)) "upstream sync throw normalized")
            (is (= :invalid-prefix (util/error-code e-sep)))
            (is (argument-error? e-opts))
            (is (codec-error? e-codec))))))))

(deftest sublevel-survives-root-reopen
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [people (core/sublevel c "people")
                  nested (core/sublevel people "nested")
                  _ (core/put! people "ann" "1")
                  _ (core/put! nested "deep" "2")
                  _ (core/close! c)
                  st (core/status people)
                  e (tu/rejection (core/get! people "ann"))
                  _ (core/open! c)
                  v (core/get! people "ann")
                  d (core/get! nested "deep")
                  _ (core/put! nested "deep2" "3")
                  ks (core/keys! nested)
                  ;; closing only the sublevel, then open! on the sublevel client
                  _ (core/close! people)
                  e2 (tu/rejection (core/get! people "ann"))
                  e3 (tu/rejection (core/get! nested "deep"))
                  root-ok (core/get! c "!people!ann")
                  _ (core/open! people)
                  v2 (core/get! people "ann")
                  d2 (core/get! nested "deep")]
            (is (= :closed st) "closing the root closes sublevel handles")
            (is (= :database-not-open (util/error-code e)))
            (is (= "1" v) "the sublevel client re-derived its handle after reopen (inventory 9.13)")
            (is (= "2" d) "nested sublevels too")
            (is (= ["deep" "deep2"] ks))
            (is (= :database-not-open (util/error-code e2)) "close! on a sublevel closes it")
            (is (= :database-not-open (util/error-code e3)) "and its descendants")
            (is (= "1" root-ok) "the root stays open")
            (is (= "1" v2) "open! on a sublevel client revives it")
            (is (= "2" d2))))))))

;; ---------------------------------------------------------------------------
;; clear!, snapshots, iterator handles, ranges
;; ---------------------------------------------------------------------------

(deftest clear-ranges-and-limits
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (tu/seed! c (into {} (for [k ["a" "b" "c" "d" "e"]] [k k])))
                  _ (core/clear! c {:gte "b" :lt "d"})
                  k1 (core/keys! c)
                  _ (core/clear! c {:limit 1 :reverse true})
                  k2 (core/keys! c)
                  _ (core/clear! c {:limit 0})
                  k3 (core/keys! c)
                  e (tu/rejection (core/clear! c {:limit -5}))
                  e2 (tu/rejection (core/clear! c {:gte nil :bogus 1}))
                  _ (core/clear! c)
                  k4 (core/keys! c)]
            (is (= ["a" "d" "e"] k1))
            (is (= ["a" "d"] k2) "reverse + limit deletes the last n")
            (is (= ["a" "d"] k3) "limit 0 deletes nothing")
            (is (argument-error? e))
            (is (= :limit (:arg (ex-data e))))
            (is (argument-error? e2))
            (is (= [] k4))))))))

(deftest explicit-snapshots
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (core/put! c "k" "old")
                  snap (core/snapshot c)
                  _ (core/put! c "k" "new")
                  _ (core/put! c "k2" "added")
                  old (core/get! c "k" {:snapshot snap})
                  has-k2 (core/has! c "k2" {:snapshot snap})
                  now (core/get! c "k")
                  snap-keys (core/keys! c {:snapshot snap})
                  res1 (core/open-resources c)
                  _ (core/close-snapshot! snap)
                  _ (core/close-snapshot! snap)
                  res2 (core/open-resources c)
                  e (tu/rejection (core/get! c "k" {:snapshot snap}))
                  via (core/with-snapshot! c (fn [s] (core/get! c "k" {:snapshot s})))
                  res3 (core/open-resources c)
                  e2 (tu/rejection (core/with-snapshot! c (fn [_] (throw (ex-info "boom" {})))))
                  res4 (core/open-resources c)]
            (is (= "old" old) "reads through the snapshot see the old state")
            (is (false? has-k2))
            (is (= "new" now))
            (is (= ["k"] snap-keys))
            (is (= 1 (:snapshots res1)))
            (is (= 0 (:snapshots res2)) "close is idempotent and releases the handle")
            (is (= :snapshot-not-open (util/error-code e)))
            (is (= "new" via))
            (is (= 0 (:snapshots res3)) "with-snapshot! closes")
            (is (= "boom" (ex-message e2)))
            (is (= 0 (:snapshots res4)) "…also on error")))))))

(deftest iterator-handles
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (tu/seed! c {"a" "1" "b" "2" "c" "3" "d" "4"})
                  it (core/iterator c)
                  first-item (core/it-next! it)
                  two (core/it-nextv! it 2)
                  cnt (core/it-count it)
                  _ (core/it-seek! it "b")
                  after-seek (core/it-next! it)
                  _ (core/it-seek! it "zzz")
                  at-end (core/it-next! it)
                  res-open (core/open-resources c)
                  open-before (core/it-open? it)
                  _ (core/it-close! it)
                  _ (core/it-close! it)
                  res-closed (core/open-resources c)
                  e-closed (tu/rejection (core/it-next! it))
                  kit (core/key-iterator c {:reverse true :limit 2})
                  ks (core/it-all! kit)
                  vit (core/value-iterator c {:gt "b"})
                  vs (core/it-all! vit)
                  e-n (tu/rejection (core/it-nextv! (core/iterator c) 0))
                  busy-it (core/iterator c)
                  e-busy (let [p1 (core/it-next! busy-it)
                               p2 (tu/rejection (core/it-next! busy-it))]
                           (p/then p1 (constantly p2)))
                  e-seek-busy (let [p (core/it-next! busy-it)
                                    e (tu/thrown #(core/it-seek! busy-it "a"))]
                                (p/then p (constantly e)))
                  _ (core/it-close! busy-it)
                  e-arg (tu/thrown #(core/iterator c {:limit -2}))
                  e-arg2 (tu/thrown #(core/iterator c {:keys-only? true :values-only? true}))
                  ko (core/all! c {:keys-only? true :limit 1})
                  vo (core/all! c {:values-only? true :limit 1})
                  final (core/open-resources c)]
            (is (= {:key "a" :value "1"} first-item))
            (is (= [{:key "b" :value "2"} {:key "c" :value "3"}] two))
            (is (= 3 cnt))
            (is (= {:key "b" :value "2"} after-seek))
            (is (nil? at-end) "seek outside the range ends the iteration")
            (is (= 1 (:iterators res-open)))
            (is (true? open-before))
            (is (= 0 (:iterators res-closed)))
            (is (not (core/it-open? it)))
            (is (= :iterator-not-open (util/error-code e-closed)))
            (is (util/not-open? e-closed))
            (is (= ["d" "c"] ks))
            (is (= ["3" "4"] vs))
            (is (argument-error? e-n))
            (is (= :iterator-busy (util/error-code e-busy)) "second read while one is in flight")
            (is (= :iterator-busy (util/error-code e-seek-busy)) "seek while busy throws synchronously")
            (is (argument-error? e-arg))
            (is (argument-error? e-arg2))
            (is (= [{:key "a"}] ko) "keys-only omits :value")
            (is (= [{:value "1"}] vo) "values-only omits :key")
            (is (= 1 (:iterators final)) "only the iterator of the rejected nextv is left open")))))))

(deftest range-edge-cases
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (tu/seed! c {"" "empty" "a" "1" "b" "2" "c" "3" "10" "ten" "9" "nine"})
                  all (core/keys! c)
                  gte (core/keys! c {:gte "b"})
                  gt (core/keys! c {:gt "b"})
                  lte (core/keys! c {:lte "b"})
                  lt (core/keys! c {:lt "b"})
                  both (core/keys! c {:gte "a" :lt "c"})
                  gte-wins (core/keys! c {:gte "b" :gt "a"})
                  lte-wins (core/keys! c {:lte "b" :lt "c"})
                  empty-lte (core/keys! c {:lte ""})
                  empty-gt (core/keys! c {:gt ""})
                  limit0 (core/keys! c {:limit 0})
                  limit-1 (core/keys! c {:limit -1})
                  limit2 (core/keys! c {:limit 2})
                  rev (core/keys! c {:reverse true :limit 2})
                  inverted (core/keys! c {:gt "c" :lt "a"})
                  nil-bounds (core/keys! c {:gt nil :lt nil})
                  vals (core/values! c {:gte "a" :lte "b"})
                  e (tu/rejection (core/keys! c {:gte 1}))]
            (is (= ["" "10" "9" "a" "b" "c"] all) "byte-wise order: '10' < '9', '' first")
            (is (= ["b" "c"] gte))
            (is (= ["c"] gt))
            (is (= ["" "10" "9" "a" "b"] lte))
            (is (= ["" "10" "9" "a"] lt))
            (is (= ["a" "b"] both))
            (is (= ["b" "c"] gte-wins) "gte beats gt")
            (is (= ["" "10" "9" "a" "b"] lte-wins) "lte beats lt")
            (is (= [""] empty-lte))
            (is (= ["10" "9" "a" "b" "c"] empty-gt))
            (is (= [] limit0) "limit 0 yields nothing")
            (is (= 6 (count limit-1)) "limit -1 is unlimited")
            (is (= ["" "10"] limit2))
            (is (= ["c" "b"] rev))
            (is (= [] inverted))
            (is (= 6 (count nil-bounds)) "nil bounds are dropped, not stringified (pitfall 8.5)")
            (is (= ["1" "2"] vals))
            (is (codec-error? e) "bounds go through the key codec")))))))

(deftest open-resources-are-released-on-close
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (core/put! c "a" "1")
                  _it (core/iterator c)
                  _s (core/snapshot c)
                  _b (core/chained-batch c)
                  before (core/open-resources c)
                  _ (core/close! c)
                  after (core/open-resources c)
                  e (tu/rejection (core/it-next! _it))]
            (is (= {:iterators 1 :snapshots 1 :batches 1 :subscriptions 0} before))
            (is (= {:iterators 0 :snapshots 0 :batches 0 :subscriptions 0} after) "close! sweeps everything")
            (is (util/not-open? e))))))))

(deftest non-client-arguments-are-argument-errors
  (async done
    (tu/finish done
      (p/let [e1 (tu/rejection (core/get! "not a client" "k"))
              e2 (tu/rejection (core/close! nil))
              e3 (tu/rejection (core/it-next! {:js 1}))
              e4 (tu/rejection (core/close-snapshot! :nope))
              e5 (tu/rejection (core/batch-write! "x"))]
        (is (argument-error? e1))
        (is (= :client (:arg (ex-data e1))))
        (is (argument-error? e2))
        (is (argument-error? e3))
        (is (argument-error? e4))
        (is (argument-error? e5))
        (is (argument-error? (tu/thrown #(core/status 1))))
        (is (argument-error? (tu/thrown #(core/sublevel nil "x"))))))))
