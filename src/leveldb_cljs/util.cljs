(ns leveldb-cljs.util
  "Small shared helpers: keyword case translation between kebab-case
   ClojureScript keywords and the camelCase option / manifest / event names
   of abstract-level, upstream LEVEL_* error-code translation, and
   ex-info error construction and detection.

   Pure: no I/O and no JavaScript interop. Converting CLJS maps to JS option
   objects (and reading `code`/`message`/`cause` off a thrown JS error) is
   interop and therefore lives in the single binding namespace
   (leveldb-cljs.core); this namespace only deals in strings, keywords and
   maps. It also holds the option normalisation and validation shared by
   the operations, and the one core.async helper shared by the iterator and
   event channels (`put-or-cancel!`)."
  (:require [clojure.string :as str]
            [cljs.core.async :as async :refer [alts! go-loop]]
            [cljs.core.async.impl.protocols :as impl]))

;; ---------------------------------------------------------------------------
;; kebab-case <-> camelCase (abstract-level options, the
;; db.supports manifest and `write` event operation objects are camelCase)
;; ---------------------------------------------------------------------------

(defn kebab->camel
  "\"key-encoding\" -> \"keyEncoding\". Keywords render as their name (the
   namespace is dropped); strings pass through untouched (they are assumed
   to be exact upstream names already). Single-word names are unchanged
   (:gte -> \"gte\")."
  [k]
  (cond
    (keyword? k) (let [[head & tail] (str/split (name k) #"-")]
                   (apply str head (map str/capitalize tail)))
    (string? k) k
    :else (str k)))

(defn camel->kebab
  "\"keyEncoding\" -> :key-encoding, \"highWaterMarkBytes\" ->
   :high-water-mark-bytes. Works on strings and keywords, returns a keyword.
   Namespaced keywords are returned unchanged."
  [k]
  (if (and (keyword? k) (namespace k))
    k
    (-> (name k)
        (str/replace #"([a-z0-9])([A-Z])" "$1-$2")
        str/lower-case
        keyword)))

(defn transform-keys
  "Recursively applies `f` to every map key in `x` (maps inside vectors/lists
   included). Keys listed in `passthrough` (a set) keep their subtree
   untouched — user-owned values (the `value` of a put, a custom encoding
   object) must never be re-cased."
  ([f x] (transform-keys f #{} x))
  ([f passthrough x]
   (cond
     (map? x) (into (empty x)
                    (map (fn [[k v]]
                           (if (contains? passthrough k)
                             [(f k) v]
                             [(f k) (transform-keys f passthrough v)])))
                    x)
     (sequential? x) (into (empty x) (map #(transform-keys f passthrough %)) x)
     :else x)))

(def option-passthrough-keys
  "Kebab keys whose subtree is handed to abstract-level verbatim: user data
   and opaque upstream objects."
  #{:value :key :gt :gte :lt :lte :snapshot :sublevel :signal})

(defn ->camel-keys
  "Kebab keyword option map -> string-keyed camelCase map, nil values
   dropped, nested option maps translated, subtrees under
   option-passthrough-keys untouched. nil in -> nil out. Pure: the result is
   still a CLJS map; the binding namespace turns it into a JS object."
  [opts]
  (when (some? opts)
    (transform-keys kebab->camel option-passthrough-keys
                    (into {} (remove (comp nil? val)) opts))))

(defn ->kebab-keys
  "Deeply translates camelCase string/keyword keys (as js->clj yields them)
   to kebab keywords; values are never touched."
  [x]
  (transform-keys camel->kebab x))

;; ---------------------------------------------------------------------------
;; Upstream error codes (LEVEL_* codes are the semver contract)
;; ---------------------------------------------------------------------------

(defn level-code->keyword
  "\"LEVEL_DATABASE_NOT_OPEN\" -> :database-not-open. The LEVEL_ prefix is
   dropped; other uppercase codes (\"ERR_X\") are kebab-cased as-is; keywords
   pass through; nil/blank -> nil."
  [code]
  (cond
    (keyword? code) code
    (and (string? code) (not (str/blank? code)))
    (-> code
        (str/replace #"^LEVEL_" "")
        str/lower-case
        (str/replace "_" "-")
        keyword)
    :else nil))

(def not-open-codes
  "Codes meaning a resource was used after close (or before a failed open)."
  #{:database-not-open :iterator-not-open :batch-not-open :snapshot-not-open})

;; ---------------------------------------------------------------------------
;; Errors
;; ---------------------------------------------------------------------------

(def error-types
  "Every :type a leveldb-cljs ex-info can carry."
  #{:leveldb-cljs/db-error :leveldb-cljs/argument-error :leveldb-cljs/codec-error})

(defn db-error
  "ex-info for an error raised by abstract-level / classic-level. `code`
   is the upstream LEVEL_* string or its keyword form; `message` the
   upstream message. Data: {:type :leveldb-cljs/db-error :code <kebab
   keyword or nil> :message .. :cause <original error, if any> ..}."
  [code message & [data]]
  (let [code (level-code->keyword code)]
    (ex-info (str "leveldb-cljs" (when code (str " " (name code))) ": " message)
             (merge {:type :leveldb-cljs/db-error
                     :code code
                     :message message}
                    data))))

(defn argument-error
  "ex-info for an invalid argument detected before any database call."
  [message & [data]]
  (ex-info (str "leveldb-cljs: " message)
           (merge {:type :leveldb-cljs/argument-error} data)))

(defn codec-error
  "ex-info for an encode/decode failure inside a leveldb-cljs codec
   (leveldb-cljs.codec). Upstream decode failures keep
   :leveldb-cljs/db-error with :code :decode-error."
  [message & [data]]
  (ex-info (str "leveldb-cljs codec: " message)
           (merge {:type :leveldb-cljs/codec-error} data)))

(defn error-data
  "Returns the structured data of a leveldb-cljs error, or nil when `e` is
   not one."
  [e]
  (when-let [d (ex-data e)]
    (when (contains? error-types (:type d))
      d)))

(defn error-type
  "One of the leveldb-cljs error :type keywords (see error-types) or nil."
  [e]
  (:type (error-data e)))

(defn error-code
  "The kebab keyword form of the upstream LEVEL_* code of a
   :leveldb-cljs/db-error, nil otherwise."
  [e]
  (when (= :leveldb-cljs/db-error (error-type e))
    (:code (error-data e))))

(defn not-open?
  "True when `e` reports use of a closed database/iterator/batch/snapshot."
  [e]
  (contains? not-open-codes (error-code e)))

;; ---------------------------------------------------------------------------
;; Options: option-key normalisation, per-method
;; option whitelists, range-option normalisation, batch-op normalisation.
;; ---------------------------------------------------------------------------

(defn strip-question-mark
  "`:create-if-missing?` -> :create-if-missing. Keywords keep their
   namespace; anything that is not a keyword is returned unchanged. Lets
   boolean options be spelled with the idiomatic `?` suffix while the
   upstream camelCase name stays `createIfMissing`."
  [k]
  (if (keyword? k)
    (keyword (namespace k) (str/replace (name k) #"\?$" ""))
    k))

(defn normalize-opts
  "Strips the `?` suffix from every top-level keyword key of `opts` and
   drops nil-valued entries. nil -> {}."
  [opts]
  (into {}
        (keep (fn [[k v]] (when (some? v) [(strip-question-mark k) v])))
        opts))

(defn check-opts!
  "Throws an argument-error when `opts` (already normalised) is not a map or
   contains a key outside `allowed` (a set of kebab keywords without `?`).
   Returns `opts`. Pitfall 8.17: upstream silently ignores unknown keys."
  [op allowed opts]
  (when-not (map? opts)
    (throw (argument-error (str (name op) ": options must be a map")
                           {:arg :opts :value opts :op op})))
  (let [unknown (remove allowed (keys opts))]
    (when (seq unknown)
      (throw (argument-error (str (name op) ": unknown option(s) " (pr-str (vec unknown))
                                  "; allowed: " (pr-str (vec (sort allowed))))
                             {:arg :opts :unknown (set unknown) :allowed allowed :op op}))))
  opts)

(def range-keys
  "The four range bounds accepted by iterators and clear!."
  #{:gt :gte :lt :lte})

(defn check-limit!
  "Validates `:limit`: nil -> unlimited (dropped), -1 ->
   unlimited, 0 -> nothing, positive integer -> that many; anything else is
   an argument-error. Returns the normalised opts."
  [op opts]
  (let [limit (:limit opts)]
    (cond
      (nil? limit) (dissoc opts :limit)
      (and (integer? limit) (or (= -1 limit) (>= limit 0))) opts
      :else (throw (argument-error (str (name op) ": :limit must be nil, -1 or a non-negative integer")
                                   {:arg :limit :value limit :op op})))))

(defn valid-op-type?
  "True for :put / :del."
  [t]
  (contains? #{:put :del} t))

(defn normalize-op
  "Validates the shape of one batch operation map:
   :type must be :put or :del, :key must be present, :put needs :value, only
   the keys #{:type :key :value :key-encoding :value-encoding :sublevel} are
   allowed. Returns the op (with `?`-less keys). Throws argument-error.
   Sublevel root identity and encoding are checked by core, which knows the
   client structure."
  [idx op]
  (when-not (map? op)
    (throw (argument-error (str "batch op " idx " must be a map") {:arg :ops :index idx :value op})))
  (let [op (normalize-opts op)
        t (:type op)]
    (when-not (valid-op-type? t)
      (throw (argument-error (str "batch op " idx ": :type must be :put or :del")
                             {:arg :ops :index idx :value t})))
    (when-not (contains? op :key)
      (throw (argument-error (str "batch op " idx ": missing :key") {:arg :ops :index idx})))
    (when (and (= :put t) (not (contains? op :value)))
      (throw (argument-error (str "batch op " idx ": :put needs a :value") {:arg :ops :index idx})))
    (check-opts! :batch #{:type :key :value :key-encoding :value-encoding :sublevel} op)
    op))

;; ---------------------------------------------------------------------------
;; The cancel-poll put shared by leveldb-cljs.iter and the events
;; channel of leveldb-cljs.core. core.async is pure
;; ClojureScript, so this stays within the no-interop rule of this namespace.
;; ---------------------------------------------------------------------------

(def cancel-poll-ms
  "How long a parked put waits before re-checking whether the consumer
   closed the channel (core.async: closing a channel does not wake parked
   puts, so a producer parked on a full buffer must poll)."
  100)

(defn chan-closed?
  "True when core.async channel `ch` has been closed."
  [ch]
  (impl/closed? ch))

(defn put-or-cancel!
  "Puts `v` on `ch`; returns a channel yielding true when delivered, false
   when the consumer closed `ch` (checked immediately and then on every
   cancel-poll-ms tick while the put is parked)."
  [ch v]
  (go-loop []
    (let [[res port] (alts! [[ch v] (async/timeout cancel-poll-ms)])]
      (cond
        (identical? port ch) (boolean res)
        (impl/closed? ch) false
        :else (recur)))))
