(ns leveldb-cljs.codec
  "Keyword-selectable value/key codecs.

   Codec discipline: every key and value leaves leveldb-cljs.core ALREADY
   ENCODED into the upstream store format (a string for `:format :utf8`,
   opaque bytes for `:format :buffer` / `:format :view`), and every raw
   read is decoded here before the caller sees it. Upstream therefore only
   ever receives its 'utf8' / 'buffer' / 'view' encodings and can never
   stringify a CLJS value silently: a non-string under
   `:utf8` is a :leveldb-cljs/codec-error, not \"[object Object]\".

   Builtin codecs:
   | keyword  | upstream | CLJS side                                   |
   |----------+----------+---------------------------------------------|
   | :utf8    | utf8     | string in, string out (default)             |
   | :json    | utf8     | CLJS data <-> JSON text (keywordized keys)  |
   | :edn     | utf8     | CLJS data <-> EDN text (pr-str/read-string) |
   | :hex     | buffer   | hex string in/out (bytes stored)            |
   | :base64  | buffer   | base64 string in/out (bytes stored)         |
   | :buffer  | buffer   | opaque bytes in, opaque Buffer out          |
   | :view    | view     | opaque bytes in, opaque Uint8Array out      |
   | :raw     | buffer   | pre-encoded string or bytes in, bytes out   |

   A custom codec is a map {:name kw :format :utf8|:buffer|:view :encode f
   :decode g [:check pred]}; encode must return a value of :format.

   This namespace holds no `level` objects. The only platform calls are
   JSON.stringify / JSON.parse for :json and Uint8Array for the bytes
   predicate; everything else is pure ClojureScript. Decoded nil (a missing
   key) passes through as nil."
  (:refer-clojure :exclude [resolve])
  (:require [cljs.reader :as reader]
            [leveldb-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; predicates
;; ---------------------------------------------------------------------------

(defn bytes?
  "True for opaque byte values (Node Buffer or any Uint8Array)."
  [x]
  (instance? js/Uint8Array x))

(def ^:private hex-re #"^([0-9a-fA-F]{2})*$")
(def ^:private base64-re #"^[A-Za-z0-9+/]*={0,2}$")

(defn hex-string?
  [x]
  (and (string? x) (some? (re-matches hex-re x))))

(defn base64-string?
  "True for a string in the base64 alphabet with valid padding."
  [x]
  (and (string? x) (zero? (mod (count x) 4)) (some? (re-matches base64-re x))))

;; ---------------------------------------------------------------------------
;; codec descriptors
;; ---------------------------------------------------------------------------

(def formats
  "The three store formats abstract-level accepts."
  #{:utf8 :buffer :view})

(def format->upstream
  "Store format keyword -> upstream encoding name."
  {:utf8 "utf8" :buffer "buffer" :view "view"})

(defn- json-encode [v]
  (let [s (js/JSON.stringify (clj->js v))]
    (when-not (string? s)
      (throw (ex-info "value has no JSON representation" {:value v})))
    s))

(defn- json-decode [s]
  (js->clj (js/JSON.parse s) :keywordize-keys true))

(defn utf8-codec
  "The default codec: strings only, stored as utf8 text."
  []
  {:name :utf8 :format :utf8 :check string? :expects "a string"
   :encode identity :decode identity})

(defn json-codec
  "CLJS data <-> JSON text. Lossy for keywords, sets, symbols and non-string
   map keys (all become strings); map keys come back as keywords."
  []
  {:name :json :format :utf8 :check any? :expects "JSON-representable data"
   :encode json-encode :decode json-decode})

(defn edn-codec
  "CLJS data <-> EDN text via pr-str / cljs.reader/read-string (the EDN
   reader: no code evaluation). Exact round-trip for all EDN data.
   Options: {:readers {'my/tag f}} for custom tagged literals, {:default f}
   for unknown tags. Recommended for CLJS values."
  ([] (edn-codec nil))
  ([{:keys [readers default]}]
   (let [opts (cond-> {:eof ::eof}
                readers (assoc :readers readers)
                default (assoc :default default))]
     {:name :edn :format :utf8 :check any? :expects "printable EDN data"
      :encode pr-str
      :decode (fn [s]
                (let [v (if (= "" s) ::eof (reader/read-string opts s))]
                  (if (= ::eof v)
                    (throw (ex-info "empty EDN string" {:value s}))
                    v)))})))

(defn hex-codec
  "Hex string in/out; the bytes are what is stored."
  []
  {:name :hex :format :buffer :check hex-string? :expects "an even-length hex string"
   :encode identity :decode identity})

(defn base64-codec
  "Base64 string in/out; the bytes are what is stored."
  []
  {:name :base64 :format :buffer :check base64-string? :expects "a base64 string"
   :encode identity :decode identity})

(defn buffer-codec
  "Opaque bytes in (Buffer / Uint8Array), opaque Buffer out. Use the bytes
   helpers in leveldb-cljs.core to build or inspect them."
  []
  {:name :buffer :format :buffer :check bytes? :expects "bytes (Buffer or Uint8Array)"
   :encode identity :decode identity})

(defn view-codec
  "Opaque bytes in, opaque Uint8Array out."
  []
  {:name :view :format :view :check bytes? :expects "bytes (Buffer or Uint8Array)"
   :encode identity :decode identity})

(defn raw-codec
  "Pass-through for pre-encoded data: a string (stored as its utf8 bytes) or
   bytes go to upstream untouched; reads yield the stored bytes (use
   core/bytes->utf8 etc.). Useful to store text produced by an external
   serializer or to read what another codec wrote, byte for byte."
  []
  {:name :raw :format :buffer
   :check #(or (string? %) (bytes? %)) :expects "a string or bytes"
   :encode identity :decode identity})

(def builtin
  "Keyword -> zero-arg codec constructor for every builtin codec."
  {:utf8 utf8-codec :json json-codec :edn edn-codec :hex hex-codec
   :base64 base64-codec :buffer buffer-codec :view view-codec :raw raw-codec})

;; :hex / :base64 hand the validated string to upstream's own 'hex' / 'base64'
;; encodings (string <-> bytes conversion happens upstream; the :check
;; predicate above is what stops a non-hex string or a CLJS map from being
;; stringified). Their store format is still :buffer.
(def ^:private upstream-override
  {:hex "hex" :base64 "base64"})

(defn resolve
  "Codec keyword or custom codec map -> validated codec map with
   {:name :format :upstream :encode :decode :check :expects}. Throws
   argument-error for unknown keywords / malformed maps."
  [x]
  (cond
    (map? x)
    (let [{:keys [name format encode decode check expects]} x]
      (when-not (contains? formats format)
        (throw (util/argument-error (str "codec :format must be one of " (pr-str formats))
                                    {:arg :codec :value x})))
      (when-not (and (fn? encode) (fn? decode))
        (throw (util/argument-error "codec needs :encode and :decode functions"
                                    {:arg :codec :value x})))
      (assoc x
             :name (or name :custom)
             :upstream (or (:upstream x) (format->upstream format))
             :check (or check any?)
             :expects (or expects "a value the codec accepts")))

    (keyword? x)
    (if-let [ctor (get builtin x)]
      (let [c (ctor)]
        (assoc c :upstream (get upstream-override x (format->upstream (:format c)))))
      (throw (util/argument-error (str "unknown codec " (pr-str x) "; builtin: "
                                       (pr-str (vec (sort (keys builtin)))))
                                  {:arg :codec :value x})))

    :else
    (throw (util/argument-error "codec must be a keyword or a codec map"
                                {:arg :codec :value x}))))

(defn codec?
  "True when `x` is a resolved codec map."
  [x]
  (and (map? x) (contains? x :upstream) (fn? (:encode x)) (fn? (:decode x))))

;; ---------------------------------------------------------------------------
;; encode / decode
;; ---------------------------------------------------------------------------

(defn check-value!
  "Validates `v` for `codec` before any upstream call: nil is an
   argument-error (LEVEL_INVALID_KEY / LEVEL_INVALID_VALUE would follow),
   a value the codec's :check rejects is a codec-error (the silent-stringify
   guard). `role` is :key or :value for the error data. Returns v."
  ([codec v] (check-value! codec v :value))
  ([codec v role]
   (when (nil? v)
     (throw (util/argument-error (str (name role) " cannot be nil") {:arg role :value v})))
   (when-not ((:check codec) v)
     (throw (util/codec-error (str "cannot encode " (name role) " with codec "
                                   (pr-str (:name codec)) ": expected " (:expects codec)
                                   ", got " (pr-str v))
                              {:codec (:name codec) :direction :encode :role role :value v})))
   v))

(defn encode
  "Encodes `v` with a resolved `codec` into its store format. Throws
   argument-error (nil) or codec-error (rejected or failing encode). `role`
   defaults to :value."
  ([codec v] (encode codec v :value))
  ([codec v role]
   (check-value! codec v role)
   (try
     ((:encode codec) v)
     (catch :default e
       (if (util/error-data e)
         (throw e)
         (throw (util/codec-error (str "encode failed for codec " (pr-str (:name codec))
                                       ": " (ex-message e))
                                  {:codec (:name codec) :direction :encode :role role
                                   :value v :cause e})))))))

(defn decode
  "Decodes a raw store value with a resolved `codec`. nil passes through
   (missing key). Failures are codec-errors carrying :cause."
  ([codec s] (decode codec s :value))
  ([codec s role]
   (when (some? s)
     (try
       ((:decode codec) s)
       (catch :default e
         (throw (util/codec-error (str "decode failed for codec " (pr-str (:name codec))
                                       ": " (ex-message e))
                                  {:codec (:name codec) :direction :decode :role role
                                   :value s :cause e})))))))

(defn describe
  "Short printable summary of a codec: {:name .. :format .. :upstream ..}."
  [codec]
  (select-keys codec [:name :format :upstream]))

(defn names
  "Sorted vector of the builtin codec keywords."
  []
  (vec (sort (keys builtin))))
