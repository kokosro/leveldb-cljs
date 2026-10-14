(ns leveldb-cljs.util-test
  (:require [cljs.test :refer [deftest is testing]]
            [clojure.string :as str]
            [leveldb-cljs.util :as util]))

(deftest kebab-camel-roundtrip
  (is (= "keyEncoding" (util/kebab->camel :key-encoding)))
  (is (= "highWaterMarkBytes" (util/kebab->camel :high-water-mark-bytes)))
  (is (= "gte" (util/kebab->camel :gte)) "single word unchanged")
  (is (= "alreadyCamel" (util/kebab->camel "alreadyCamel")) "strings pass through")
  (is (= "createIfMissing" (util/kebab->camel :leveldb-cljs/create-if-missing)) "namespace dropped")
  (is (= :key-encoding (util/camel->kebab "keyEncoding")))
  (is (= :high-water-mark-bytes (util/camel->kebab :highWaterMarkBytes)))
  (is (= :encoded-key (util/camel->kebab "encodedKey")))
  (is (= :gte (util/camel->kebab "gte")))
  (is (= :leveldb-cljs/x (util/camel->kebab :leveldb-cljs/x)) "namespaced keywords untouched")
  (doseq [k [:key-encoding :value-encoding :create-if-missing :error-if-exists
             :fill-cache :high-water-mark-bytes :write-buffer-size :max-open-files
             :block-restart-interval :reverse :limit]]
    (is (= k (util/camel->kebab (util/kebab->camel k))) (str "roundtrip " k))))

(deftest transform-keys-honours-passthrough
  (is (= {"aB" {"cD" 1} "value" {:x-y 2}}
         (util/transform-keys util/kebab->camel #{:value} {:a-b {:c-d 1} :value {:x-y 2}})))
  (is (= [{:a-b 1}] (util/transform-keys util/camel->kebab [{:aB 1}]))))

(deftest ->camel-keys-drops-nil-and-keeps-user-data
  (let [o (util/->camel-keys {:key-encoding "utf8" :value-encoding :json :limit nil
                              :gte {:not-touched 1} :value {:user-key 1}
                              :nested {:fill-cache false}})]
    (is (= {"keyEncoding" "utf8" "valueEncoding" :json
            "gte" {:not-touched 1} "value" {:user-key 1}
            "nested" {"fillCache" false}}
           o)
        "keys camelised, nil dropped, values untouched, passthrough subtrees kept")
    (is (nil? (util/->camel-keys nil)))))

(deftest ->kebab-keys-translates-manifests
  (is (= {:implicit-snapshots true :encodings {:utf8 true :json true}
          :additional-methods {:approximate-size true}}
         (util/->kebab-keys {"implicitSnapshots" true "encodings" {"utf8" true "json" true}
                             "additionalMethods" {"approximateSize" true}}))))

(deftest level-codes-become-keywords
  (is (= :database-not-open (util/level-code->keyword "LEVEL_DATABASE_NOT_OPEN")))
  (is (= :iterator-busy (util/level-code->keyword "LEVEL_ITERATOR_BUSY")))
  (is (= :decode-error (util/level-code->keyword "LEVEL_DECODE_ERROR")))
  (is (= :locked (util/level-code->keyword "LEVEL_LOCKED")))
  (is (= :err-x (util/level-code->keyword "ERR_X")) "non-LEVEL codes kebab-cased")
  (is (= :locked (util/level-code->keyword :locked)) "keywords pass through")
  (is (nil? (util/level-code->keyword nil)))
  (is (nil? (util/level-code->keyword ""))))

(deftest error-constructors-and-detection
  (let [db (util/db-error "LEVEL_DATABASE_NOT_OPEN" "Database is not open" {:cause :js-err})
        db2 (util/db-error :locked "locked")
        db3 (util/db-error nil "no code")
        arg (util/argument-error "bad" {:arg :key})
        codec (util/codec-error "not edn" {:codec :edn})]
    (is (= :leveldb-cljs/db-error (util/error-type db)))
    (is (= :database-not-open (util/error-code db)))
    (is (= :database-not-open (:code (util/error-data db))))
    (is (= "Database is not open" (:message (util/error-data db))))
    (is (= :js-err (:cause (util/error-data db))))
    (is (= "leveldb-cljs database-not-open: Database is not open" (ex-message db)))
    (is (util/not-open? db))
    (is (= :locked (util/error-code db2)))
    (is (not (util/not-open? db2)))
    (is (nil? (util/error-code db3)))
    (is (= "leveldb-cljs: no code" (ex-message db3)))
    (is (= :leveldb-cljs/argument-error (util/error-type arg)))
    (is (= :key (:arg (ex-data arg))))
    (is (nil? (util/error-code arg)) "only db errors carry codes")
    (is (= :leveldb-cljs/codec-error (util/error-type codec)))
    (is (str/starts-with? (ex-message codec) "leveldb-cljs codec:"))
    (is (nil? (util/error-data (ex-info "other" {:type :something/else}))))
    (is (nil? (util/error-data (js/Error. "plain"))))))
