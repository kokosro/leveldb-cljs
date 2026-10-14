(ns leveldb-cljs.codec-test
  (:require [cljs.test :refer [deftest is testing]]
            [clojure.string :as str]
            [leveldb-cljs.codec :as codec]
            [leveldb-cljs.core :as core]
            [leveldb-cljs.test-util :as tu]
            [leveldb-cljs.util :as util]))

(defn- codec-error? [e]
  (= :leveldb-cljs/codec-error (util/error-type e)))

(defn- argument-error? [e]
  (= :leveldb-cljs/argument-error (util/error-type e)))

(deftest builtin-codecs-resolve
  (is (= [:base64 :buffer :edn :hex :json :raw :utf8 :view] (codec/names)))
  (doseq [k (codec/names)]
    (let [c (codec/resolve k)]
      (is (codec/codec? c) (str k " resolves"))
      (is (= k (:name c)))
      (is (contains? codec/formats (:format c)))
      (is (string? (:upstream c)))))
  (is (= {:name :utf8 :format :utf8 :upstream "utf8"} (codec/describe (codec/resolve :utf8))))
  (is (= {:name :json :format :utf8 :upstream "utf8"} (codec/describe (codec/resolve :json)))
      "json is a library-side codec over utf8 text")
  (is (= {:name :edn :format :utf8 :upstream "utf8"} (codec/describe (codec/resolve :edn))))
  (is (= {:name :hex :format :buffer :upstream "hex"} (codec/describe (codec/resolve :hex))))
  (is (= {:name :base64 :format :buffer :upstream "base64"} (codec/describe (codec/resolve :base64))))
  (is (= {:name :buffer :format :buffer :upstream "buffer"} (codec/describe (codec/resolve :buffer))))
  (is (= {:name :view :format :view :upstream "view"} (codec/describe (codec/resolve :view))))
  (is (= {:name :raw :format :buffer :upstream "buffer"} (codec/describe (codec/resolve :raw))))
  (is (codec/codec? (codec/resolve (codec/resolve :edn))) "resolving a resolved codec is a no-op"))

(deftest resolve-rejects-unknown-and-malformed
  (let [e (tu/thrown #(codec/resolve :transit))]
    (is (argument-error? e))
    (is (= :codec (:arg (ex-data e))))
    (is (str/includes? (ex-message e) ":transit")))
  (is (argument-error? (tu/thrown #(codec/resolve "utf8"))) "strings are not codec names")
  (is (argument-error? (tu/thrown #(codec/resolve nil))))
  (is (argument-error? (tu/thrown #(codec/resolve {:format :utf8 :encode identity}))) "missing :decode")
  (is (argument-error? (tu/thrown #(codec/resolve {:format :text :encode identity :decode identity})))
      "bad :format"))

(deftest custom-codec-map
  (let [upper (codec/resolve {:name :upper :format :utf8
                              :check string?
                              :encode str/upper-case
                              :decode str/lower-case})]
    (is (= "HELLO" (codec/encode upper "hello")))
    (is (= "hello" (codec/decode upper "HELLO")))
    (is (= "utf8" (:upstream upper)))
    (is (codec-error? (tu/thrown #(codec/encode upper 42))) ":check enforced"))
  (let [anon (codec/resolve {:format :buffer :encode core/utf8->bytes :decode core/bytes->utf8})]
    (is (= :custom (:name anon)))
    (is (= "buffer" (:upstream anon)))
    (is (= "x" (codec/decode anon (codec/encode anon "x"))))))

(deftest utf8-round-trip-and-silent-stringify-guard
  (let [c (codec/resolve :utf8)]
    (is (= "plain" (codec/encode c "plain")))
    (is (= "" (codec/encode c "")) "empty string is a valid utf8 value")
    (is (= "plain" (codec/decode c "plain")))
    (testing "non-strings are codec errors, never \"[object Object]\""
      (doseq [bad [{:a 1} [1 2] 42 :kw 'sym true (core/utf8->bytes "b")]]
        (let [e (tu/thrown #(codec/encode c bad))]
          (is (codec-error? e) (str "rejects " (pr-str bad)))
          (is (= :utf8 (:codec (ex-data e))))
          (is (= :encode (:direction (ex-data e))))
          (is (= bad (:value (ex-data e)))))))
    (testing "nil is an argument error carrying the role"
      (let [e (tu/thrown #(codec/encode c nil :key))]
        (is (argument-error? e))
        (is (= :key (:arg (ex-data e)))))
      (is (= :value (:arg (ex-data (tu/thrown #(codec/encode c nil)))))))))

(deftest json-round-trip
  (let [c (codec/resolve :json)
        data {:name "ann" :tags ["a" "b"] :nested {:n 1.5 :ok true :none nil} :list [1 [2 3]]}]
    (is (= "{\"a\":1}" (codec/encode c {:a 1})) "a real JSON string, not a persistent map dump")
    (is (= data (codec/decode c (codec/encode c data))) "keywordized keys round-trip")
    (is (= [1 "two" nil] (codec/decode c "[1,\"two\",null]")))
    (is (= "s" (codec/decode c "\"s\"")))
    (is (= "42" (codec/encode c 42)) "JSON scalars encode to their text")
    (is (= "\"str\"" (codec/encode c "str")))
    (let [e (tu/thrown #(codec/decode c "{not json"))]
      (is (codec-error? e))
      (is (= :decode (:direction (ex-data e))))
      (is (= :json (:codec (ex-data e))))
      (is (some? (:cause (ex-data e))) "carries the JSON.parse error"))
    (let [e (tu/thrown #(codec/encode c (fn [])))]
      (is (codec-error? e) "a function has no JSON representation"))))

(deftest edn-round-trip
  (let [c (codec/resolve :edn)
        data {:k :kw :s #{1 2} :v [1 2.5 "s" nil true] :m {"str-key" 'sym} :l '(1 2) :n -3}]
    (is (= (pr-str data) (codec/encode c data)))
    (is (= data (codec/decode c (codec/encode c data))) "exact round-trip")
    (is (= "" (codec/decode c "\"\"")))
    (is (= 1 (codec/decode c "1")))
    (let [e (tu/thrown #(codec/decode c "{:unbalanced"))]
      (is (codec-error? e))
      (is (= :edn (:codec (ex-data e))))
      (is (= :decode (:direction (ex-data e)))))
    (is (codec-error? (tu/thrown #(codec/decode c ""))) "empty EDN text is an error")
    (is (codec-error? (tu/thrown #(codec/decode c "#js/unknown 1"))) "unknown tag without :default")
    (testing "custom readers"
      (let [with-readers (codec/resolve (codec/edn-codec {:readers {'my/inc inc}}))]
        (is (= 2 (codec/decode with-readers "#my/inc 1"))))
      (let [with-default (codec/resolve (codec/edn-codec {:default (fn [tag v] [tag v])}))]
        (is (= ['unknown/tag 7] (codec/decode with-default "#unknown/tag 7")))))))

(deftest hex-and-base64-validation
  (let [h (codec/resolve :hex) b (codec/resolve :base64)]
    (is (= "deadbeef" (codec/encode h "deadbeef")))
    (is (= "DEADBEEF" (codec/encode h "DEADBEEF")))
    (is (= "" (codec/encode h "")))
    (is (codec-error? (tu/thrown #(codec/encode h "abc"))) "odd length")
    (is (codec-error? (tu/thrown #(codec/encode h {:a 1}))))
    (is (= "aGk=" (codec/encode b "aGk=")))
    (is (= "" (codec/encode b "")))
    (is (codec-error? (tu/thrown #(codec/encode b "aGk"))) "bad padding length")
    (is (codec-error? (tu/thrown #(codec/encode b "a$k="))) "bad alphabet")
    (is (codec-error? (tu/thrown #(codec/encode b 12))))))

(deftest buffer-view-raw-checks
  (let [bytes (core/utf8->bytes "hi")
        buf (codec/resolve :buffer) view (codec/resolve :view) raw (codec/resolve :raw)]
    (is (codec/bytes? bytes))
    (is (not (codec/bytes? "hi")))
    (is (identical? bytes (codec/encode buf bytes)) "bytes pass through untouched")
    (is (identical? bytes (codec/encode view bytes)))
    (is (codec-error? (tu/thrown #(codec/encode buf "hi"))) ":buffer rejects strings")
    (is (codec-error? (tu/thrown #(codec/encode view {:a 1}))))
    (is (identical? bytes (codec/encode raw bytes)))
    (is (= "text" (codec/encode raw "text")) ":raw accepts pre-encoded text")
    (is (codec-error? (tu/thrown #(codec/encode raw {:a 1}))))
    (is (identical? bytes (codec/decode buf bytes)))))

(deftest decode-nil-passes-through
  (doseq [k (codec/names)]
    (is (nil? (codec/decode (codec/resolve k) nil)) (str k " decodes nil to nil (missing key)"))))

(deftest encode-failures-are-wrapped
  (let [boom (codec/resolve {:name :boom :format :utf8
                             :encode (fn [_] (throw (js/Error. "kaboom")))
                             :decode (fn [_] (throw (js/Error. "kadecode")))})]
    (let [e (tu/thrown #(codec/encode boom "x"))]
      (is (codec-error? e))
      (is (str/includes? (ex-message e) "kaboom"))
      (is (= :boom (:codec (ex-data e))))
      (is (some? (:cause (ex-data e)))))
    (let [e (tu/thrown #(codec/decode boom "x"))]
      (is (codec-error? e))
      (is (= :decode (:direction (ex-data e))))))
  (let [arg (codec/resolve {:name :arg :format :utf8
                            :encode (fn [_] (throw (util/argument-error "mine" {:arg :x})))
                            :decode identity})
        e (tu/thrown #(codec/encode arg "x"))]
    (is (argument-error? e) "leveldb-cljs errors thrown by a codec pass through unchanged")
    (is (= :x (:arg (ex-data e))))))

(deftest check-value-returns-value
  (is (= "v" (codec/check-value! (codec/resolve :utf8) "v")))
  (is (= {:a 1} (codec/check-value! (codec/resolve :edn) {:a 1})))
  (is (argument-error? (tu/thrown #(codec/check-value! (codec/resolve :edn) nil :key)))))
