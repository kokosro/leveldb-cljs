(ns leveldb-cljs.core
  "The leveldb-cljs client: the ONLY namespace that touches `level` /
   `memory-level` objects (js/require, method calls, JS option objects,
   Buffers). Everything it exposes takes and returns plain ClojureScript:
   kebab-case keyword maps, promesa promises for every I/O operation,
   opaque record handles for databases, iterators, chained batches and
   snapshots. leveldb-cljs.iter builds core.async channels on top of the
   iterator handles defined here; the events channel (`events!`) lives in
   this namespace because subscribing is interop.

   Conventions:
   - names ending in `!` perform I/O and return a promise, except the
     documented synchronous ones (`get-sync`, `it-seek!`, `batch-put!`,
     `batch-del!`, `batch-clear!`), which return a value and throw;
   - constructors (`sublevel`, `iterator`, `chained-batch`, `snapshot`) are
     synchronous and throw normalized errors;
   - every error, whether upstream threw synchronously or rejected, arrives
     as ONE of the util taxonomy: :leveldb-cljs/db-error (with :code, the
     kebab form of the upstream LEVEL_* code, :cause = the upstream error,
     :cause-code / :cause-message from its cause, :op, and a best-effort
     :reason for failed opens), :leveldb-cljs/argument-error (validation
     BEFORE any upstream call) or :leveldb-cljs/codec-error;
   - codec discipline: keys, values and range bounds are encoded by
     leveldb-cljs.codec before upstream sees them and decoded after; upstream
     only ever receives its 'utf8' / 'buffer' / 'view' (and 'hex' / 'base64'
     for the string-bytes codecs) encodings, so it can never stringify a
     CLJS value silently.

   Lifecycle: `open!` awaits the upstream open, so a client handed out is
   always :open and chained batches always work. `close!` closes the root
   and every sublevel, iterator, batch and snapshot. Reopening (`open!` on
   a closed client) bumps an open generation; sublevel clients re-derive
   their upstream handle lazily when the generation changed (upstream
   sublevel handles stay closed forever after a parent close)."
  (:require [clojure.string :as str]
            [cljs.core.async :as async :refer [<! alts! go-loop]]
            [promesa.core :as p]
            [leveldb-cljs.codec :as codec]
            [leveldb-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; error normalization
;; ---------------------------------------------------------------------------

(defn- js-prop
  "Property `k` of JS object `o`, nil for nil/undefined/absent."
  [o k]
  (when (some? o)
    (let [v (unchecked-get o k)]
      (when-not (undefined? v) v))))

(defn- open-failure-reason
  "Heuristic for :database-not-open errors: :locked /
   :missing / :exists / :io / nil, from the cause code or message text."
  [cause-code cause-message]
  (cond
    (= :locked cause-code) :locked
    (nil? cause-message) nil
    (str/includes? cause-message "does not exist") :missing
    (str/includes? cause-message "exists") :exists
    (str/includes? cause-message "IO error") :io
    :else nil))

(defn- ->error
  "Anything thrown/rejected -> a leveldb-cljs ex-info. leveldb-cljs errors
   and any other ex-info (user code inside with-batch! / with-snapshot!)
   pass through untouched; JS errors (upstream ModuleErrors, TypeErrors)
   become db-errors."
  [op e]
  (if (or (util/error-data e) (instance? ExceptionInfo e))
    e
    (let [code (js-prop e "code")
          message (or (js-prop e "message") (str e))
          ename (js-prop e "name")
          cause (js-prop e "cause")
          cause-code (util/level-code->keyword (js-prop cause "code"))
          cause-message (js-prop cause "message")
          code-kw (util/level-code->keyword code)
          reason (when (= :database-not-open code-kw)
                   (open-failure-reason cause-code cause-message))]
      (util/db-error code message
                     (cond-> {:cause e :op op}
                       (string? ename) (assoc :name ename)
                       (some? cause-code) (assoc :cause-code cause-code)
                       (some? cause-message) (assoc :cause-message cause-message)
                       (some? reason) (assoc :reason reason))))))

(defn- run
  "Runs `f` (which may throw a leveldb-cljs or upstream error synchronously
   and returns a promise or a value) and returns a promesa promise that
   rejects with a normalized error. The single error-delivery adapter."
  [op f]
  (try
    (p/catch (p/promise (f)) (fn [e] (throw (->error op e))))
    (catch :default e
      (p/rejected (->error op e)))))

(defn- run-sync
  "Synchronous counterpart of `run`: returns the value of (f) or throws a
   normalized error."
  [op f]
  (try
    (f)
    (catch :default e
      (throw (->error op e)))))

(defn- undefined->nil [v]
  (if (undefined? v) nil v))

;; ---------------------------------------------------------------------------
;; handles
;; ---------------------------------------------------------------------------

(defrecord Client
  ;; kind: :root or :sublevel. root: the root Client (nil for a root).
  ;; parent/name/sub-opts: sublevel linkage. key-codec/value-codec: resolved
  ;; codec maps. state: atom — root: {:db :generation :location :backend
  ;; :iterators :snapshots :batches}; sublevel: {:handle :gen}.
  [kind root parent name sub-opts key-codec value-codec state])

(defrecord Iterator [client kind key-codec value-codec js state])
(defrecord ChainedBatch [client js state])
(defrecord Snapshot [client js state])

(defn- write-handle [w tag & kvs]
  (-write w (str "#leveldb-cljs/" tag (pr-str (apply array-map kvs)))))

;; Handles hold upstream JS objects (with cycles): print a short summary
;; instead of walking them.
(extend-protocol IPrintWithWriter
  Client
  (-pr-writer [c w _]
    (write-handle w "client" :kind (:kind c) :name (:name c)
                  :key-encoding (:name (:key-codec c)) :value-encoding (:name (:value-codec c))))
  Iterator
  (-pr-writer [it w _]
    (write-handle w "iterator" :kind (:kind it) :open? (boolean (:open? @(:state it)))))
  ChainedBatch
  (-pr-writer [b w _]
    (write-handle w "chained-batch" :open? (boolean (:open? @(:state b)))))
  Snapshot
  (-pr-writer [s w _]
    (write-handle w "snapshot" :open? (boolean (:open? @(:state s))))))

(defn client?
  "True for a client (root or sublevel) produced by open! / sublevel."
  [x]
  (instance? Client x))

(defn root?
  "True for a root client (one created by open!, not by sublevel)."
  [c]
  (and (client? c) (= :root (:kind c))))

(defn root
  "The root client of `c` (itself for a root client)."
  [c]
  (if (root? c) c (:root c)))

(defn parent
  "The parent client of a sublevel client, nil for a root."
  [c]
  (:parent c))

(defn- check-client! [op c]
  (when-not (client? c)
    (throw (util/argument-error (str (name op) ": first argument must be a client")
                                {:arg :client :value c :op op}))))

(defn- generation [c]
  (:generation @(:state (root c))))

(defn- js-sub-name [n]
  (if (string? n) n (to-array n)))

(defn- js-opts
  "Kebab option map (already normalised and encoded) -> JS options object."
  [opts]
  (clj->js (util/->camel-keys opts)))

(defn- js-db
  "The current upstream handle of `c`. Sublevel handles are re-derived from
   the parent's current handle on first use, whenever the root's open
   generation moved (root reopened) or the parent's handle changed (parent
   sublevel reopened); the derivation is synchronous and may throw upstream
   errors (e.g. LEVEL_INVALID_PREFIX), which callers normalize."
  [c]
  (if (root? c)
    (:db @(:state c))
    (let [gen (generation c)
          parent-handle (js-db (:parent c))
          cache @(:state c)]
      (if (and (some? (:handle cache))
               (= gen (:gen cache))
               (identical? parent-handle (:parent-handle cache)))
        (:handle cache)
        (let [h (js-invoke parent-handle "sublevel" (js-sub-name (:name c))
                           (js-opts (select-keys (:sub-opts c) [:separator])))]
          (swap! (:state c) assoc :handle h :gen gen :parent-handle parent-handle)
          h)))))

;; ---------------------------------------------------------------------------
;; options and codecs
;; ---------------------------------------------------------------------------

(def open-options
  "Options accepted by open! (kebab, `?` suffix optional on booleans).
   :path (alias :location), :memory (memory-level backend, tests),
   :key-encoding / :value-encoding (codec keyword or map), and the upstream
   open / classic-level tuning options passed through as camelCase."
  #{:path :location :memory :key-encoding :value-encoding
    :create-if-missing :error-if-exists :compression :cache-size
    :write-buffer-size :block-size :max-open-files :block-restart-interval
    :max-file-size :multithreading :store-encoding})

(def read-options #{:key-encoding :value-encoding :snapshot :fill-cache})
(def has-options #{:key-encoding :snapshot})
(def put-options #{:key-encoding :value-encoding :sync})
(def del-options #{:key-encoding :sync})
(def batch-options #{:key-encoding :value-encoding :sync})
(def batch-op-options #{:key-encoding :value-encoding :sublevel})
(def iterator-options
  #{:gt :gte :lt :lte :reverse :limit :keys-only :values-only :key-encoding
    :value-encoding :snapshot :fill-cache :high-water-mark-bytes})
(def clear-options #{:gt :gte :lt :lte :reverse :limit :key-encoding :snapshot})
(def sublevel-options #{:separator :key-encoding :value-encoding})
(def seek-options #{:key-encoding})
(def write-options #{:sync})

(defn- opts!
  "normalize + whitelist in one step."
  [op allowed opts]
  (util/check-opts! op allowed (util/normalize-opts opts)))

(defn- key-codec [c opts]
  (if-some [e (:key-encoding opts)] (codec/resolve e) (:key-codec c)))

(defn- value-codec [c opts]
  (if-some [e (:value-encoding opts)] (codec/resolve e) (:value-codec c)))

(defn- snapshot-js
  "The upstream snapshot object for a :snapshot option (nil when absent)."
  [op s]
  (when (some? s)
    (when-not (instance? Snapshot s)
      (throw (util/argument-error (str (name op) ": :snapshot must be a snapshot handle")
                                  {:arg :snapshot :value s :op op})))
    (:js s)))

(defn- upstream-opts
  "Replaces the codec options by the upstream encoding names and the
   snapshot handle by its JS object; returns a kebab map ready for js-opts."
  [op opts kc vc]
  (cond-> (dissoc opts :key-encoding :value-encoding :snapshot)
    kc (assoc :key-encoding (:upstream kc))
    vc (assoc :value-encoding (:upstream vc))
    (contains? opts :snapshot) (assoc :snapshot (snapshot-js op (:snapshot opts)))))

(defn- encode-bounds
  "Encodes the :gt/:gte/:lt/:lte bounds present in opts with the key codec."
  [kc opts]
  (reduce (fn [m k]
            (if (contains? m k)
              (assoc m k (codec/encode kc (get m k) :key))
              m))
          opts
          util/range-keys))

;; ---------------------------------------------------------------------------
;; lifecycle
;; ---------------------------------------------------------------------------

(defn- load-ctor
  "Lazily requires the backend module and returns its
   constructor."
  [backend]
  (case backend
    :memory (unchecked-get (js/require "memory-level") "MemoryLevel")
    :level (unchecked-get (js/require "level") "Level")))

(def ^:private track-keys
  {:iterator :iterators :snapshot :snapshots :batch :batches :subscription :subscriptions})

(defn- track! [c kind handle]
  (swap! (:state (root c)) update (track-keys kind) (fnil conj #{}) handle)
  handle)

(defn- untrack! [c kind handle]
  (swap! (:state (root c)) update (track-keys kind) disj handle)
  handle)

(defn open-resources
  "Counts of the iterator / snapshot / chained-batch handles and events!
   subscriptions created through this client's root that have not been
   closed / released yet: {:iterators n :snapshots n :batches n
   :subscriptions n}. Useful to prove that a cancelled iterator channel
   released its upstream iterator, or that a closed events channel removed
   its upstream listeners."
  [c]
  (check-client! :open-resources c)
  (let [s @(:state (root c))]
    {:iterators (count (:iterators s))
     :snapshots (count (:snapshots s))
     :batches (count (:batches s))
     :subscriptions (count (:subscriptions s))}))

(defn- open-new!
  [opts]
  (run :open
       (fn []
         (let [opts (opts! :open open-options opts)
               path (or (:path opts) (:location opts))
               backend (if (:memory opts) :memory :level)
               kc (codec/resolve (get opts :key-encoding :utf8))
               vc (codec/resolve (get opts :value-encoding :utf8))
               ctor-opts (js-opts (dissoc opts :path :location :memory :key-encoding :value-encoding))]
           (when (and (= :level backend) (not (string? path)))
             (throw (util/argument-error "open!: :path (or :location) must be a string (or use :memory? true)"
                                         {:arg :path :value path :op :open})))
           (let [ctor (load-ctor backend)
                 db (if (= :memory backend) (new ctor ctor-opts) (new ctor path ctor-opts))
                 client (->Client :root nil nil nil nil kc vc
                                  (atom {:db db :generation 0 :location path :backend backend
                                         :iterators #{} :snapshots #{} :batches #{}
                                         :subscriptions #{}}))]
             (p/then (js-invoke db "open")
                     (fn [_]
                       (swap! (:state client) update :generation inc)
                       client)))))))

(defn- reopen!
  [c]
  (let [r (root c)]
    (run :open
         (fn []
           (let [db (js-db r)
                 was-open? (= "open" (js-prop db "status"))]
             (p/then (js-invoke db "open")
                     (fn [_]
                       ;; a real reopen invalidates every sublevel handle (they stay
                       ;; closed upstream); an idempotent open! on an
                       ;; open root must not churn handles
                       (when-not was-open?
                         (swap! (:state r) update :generation inc))
                       (when-not (root? c)
                         (swap! (:state c) assoc :handle nil))
                       c)))))))

(defn open!
  "Opens a database and resolves to an already-open client.
   - a string: the directory path of a `level` (classic-level) database;
   - a map: {:path \"dir\" (alias :location) :memory? false :key-encoding :utf8
     :value-encoding :utf8 :create-if-missing? true :error-if-exists? false
     :compression :cache-size :write-buffer-size :block-size :max-open-files
     :block-restart-interval :max-file-size :multithreading :store-encoding};
     :memory? true selects the in-process `memory-level` backend (tests;
     :create-if-missing? / :error-if-exists? are then not supported upstream);
     encodings are codec keywords or maps (leveldb-cljs.codec); unknown option
     keys are an argument-error;
   - a client: reopens a closed database (idempotent when already open);
     sublevel clients reopen their root and get a fresh upstream handle.
   The upstream `open()` is awaited so failures (`:code :database-not-open`
   with :reason :locked / :missing / :exists / :io and :cause-message)
   surface here rather than on the first deferred operation."
  [x]
  (cond
    (client? x) (reopen! x)
    (string? x) (open-new! {:path x})
    (map? x) (open-new! x)
    :else (p/rejected (util/argument-error "open!: expected a path string, an options map or a client"
                                          {:arg :opts :value x :op :open}))))

(defn close!
  "Closes the database (root client: everything, including sublevels,
   iterators, chained batches and snapshots; sublevel client: only that
   sublevel handle and its descendants). Idempotent. Resolves to nil.
   Operations on a closed client reject with :code :database-not-open."
  [c]
  (run :close
       (fn []
         (check-client! :close c)
         (p/then (js-invoke (js-db c) "close")
                 (fn [_]
                   (when (root? c)
                     (swap! (:state c) assoc :iterators #{} :snapshots #{} :batches #{}))
                   nil)))))

(defn status
  "Upstream status keyword of the client's current handle: :opening / :open
   / :closing / :closed. Synchronous."
  [c]
  (check-client! :status c)
  (run-sync :status #(keyword (js-prop (js-db c) "status"))))

(defn open?
  "True when (status c) is :open."
  [c]
  (= :open (status c)))

(defn supports
  "The upstream `db.supports` manifest as a kebab-keyword map
   (:implicit-snapshots, :explicit-snapshots, :encodings {...},
   :additional-methods {...}, ...). Synchronous."
  [c]
  (check-client! :supports c)
  (run-sync :supports #(util/->kebab-keys (js->clj (js-prop (js-db c) "supports")))))

(defn location
  "The directory path of the root database (nil for the memory backend)."
  [c]
  (check-client! :location c)
  (:location @(:state (root c))))

(defn backend
  "The backend keyword of the root: :level or :memory."
  [c]
  (check-client! :backend c)
  (:backend @(:state (root c))))

(defn key-encoding
  "Codec summary of the client's default key codec: {:name :format :upstream}."
  [c]
  (codec/describe (:key-codec c)))

(defn value-encoding
  "Codec summary of the client's default value codec."
  [c]
  (codec/describe (:value-codec c)))

;; ---------------------------------------------------------------------------
;; reads
;; ---------------------------------------------------------------------------

(defn get!
  "Value stored under `key`, decoded with the value codec, or nil when the
   key is absent (upstream resolves undefined; there is no not-found
   error). opts: :key-encoding, :value-encoding (per-call codec overrides),
   :snapshot (a snapshot handle), :fill-cache."
  [c key & [opts]]
  (run :get
       (fn []
         (check-client! :get c)
         (let [opts (opts! :get read-options opts)
               kc (key-codec c opts)
               vc (value-codec c opts)
               ek (codec/encode kc key :key)
               jo (js-opts (upstream-opts :get opts kc vc))]
           (p/then (js-invoke (js-db c) "get" ek jo)
                   (fn [v] (codec/decode vc (undefined->nil v))))))))

(defn get-sync
  "Synchronous `get!`: returns the decoded value or nil, throws normalized
   errors. CAVEAT: this BLOCKS THE NODE EVENT LOOP for the duration of the
   LevelDB read (upstream `getSync`) — nothing else runs
   meanwhile. Faster than get! for small hot reads; never use it in a
   request path that must stay responsive. Same opts as get!."
  [c key & [opts]]
  (run-sync :get-sync
            (fn []
              (check-client! :get-sync c)
              (let [opts (opts! :get-sync read-options opts)
                    kc (key-codec c opts)
                    vc (value-codec c opts)
                    ek (codec/encode kc key :key)
                    jo (js-opts (upstream-opts :get-sync opts kc vc))]
                (codec/decode vc (undefined->nil (js-invoke (js-db c) "getSync" ek jo)))))))

(defn get-many!
  "Vector of decoded values for a sequence of keys, nil at the positions of
   absent keys. Same opts as get!."
  [c keys & [opts]]
  (run :get-many
       (fn []
         (check-client! :get-many c)
         (when-not (sequential? keys)
           (throw (util/argument-error "get-many!: keys must be a sequential collection"
                                       {:arg :keys :value keys :op :get-many})))
         (let [opts (opts! :get-many read-options opts)
               kc (key-codec c opts)
               vc (value-codec c opts)
               eks (to-array (map #(codec/encode kc % :key) keys))
               jo (js-opts (upstream-opts :get-many opts kc vc))]
           (p/then (js-invoke (js-db c) "getMany" eks jo)
                   (fn [vs] (mapv #(codec/decode vc (undefined->nil %)) (array-seq vs))))))))

(defn has!
  "Promise of true/false: whether `key` exists. opts: :key-encoding, :snapshot."
  [c key & [opts]]
  (run :has
       (fn []
         (check-client! :has c)
         (let [opts (opts! :has has-options opts)
               kc (key-codec c opts)
               ek (codec/encode kc key :key)
               jo (js-opts (upstream-opts :has opts kc nil))]
           (p/then (js-invoke (js-db c) "has" ek jo) boolean)))))

(defn has-many!
  "Vector of booleans for a sequence of keys. opts as has!."
  [c keys & [opts]]
  (run :has-many
       (fn []
         (check-client! :has-many c)
         (when-not (sequential? keys)
           (throw (util/argument-error "has-many!: keys must be a sequential collection"
                                       {:arg :keys :value keys :op :has-many})))
         (let [opts (opts! :has-many has-options opts)
               kc (key-codec c opts)
               eks (to-array (map #(codec/encode kc % :key) keys))
               jo (js-opts (upstream-opts :has-many opts kc nil))]
           (p/then (js-invoke (js-db c) "hasMany" eks jo)
                   (fn [bs] (mapv boolean (array-seq bs))))))))

;; ---------------------------------------------------------------------------
;; writes
;; ---------------------------------------------------------------------------

(defn put!
  "Stores `value` under `key` (both encoded by the codecs; nil is an
   argument-error, a value the codec rejects is a codec-error). opts:
   :key-encoding, :value-encoding, :sync (fsync, classic-level). Resolves nil."
  [c key value & [opts]]
  (run :put
       (fn []
         (check-client! :put c)
         (let [opts (opts! :put put-options opts)
               kc (key-codec c opts)
               vc (value-codec c opts)
               ek (codec/encode kc key :key)
               ev (codec/encode vc value :value)
               jo (js-opts (upstream-opts :put opts kc vc))]
           (p/then (js-invoke (js-db c) "put" ek ev jo) (constantly nil))))))

(defn del!
  "Deletes `key` (deleting an absent key resolves normally). opts:
   :key-encoding, :sync. Resolves nil."
  [c key & [opts]]
  (run :del
       (fn []
         (check-client! :del c)
         (let [opts (opts! :del del-options opts)
               kc (key-codec c opts)
               ek (codec/encode kc key :key)
               jo (js-opts (upstream-opts :del opts kc nil))]
           (p/then (js-invoke (js-db c) "del" ek jo) (constantly nil))))))

(defn- op-sublevel!
  "Validates the :sublevel of a batch op: a sublevel client sharing the root
   of `c` (upstream silently loses writes to a foreign root).
   Returns the sublevel client or nil."
  [op c s]
  (when (some? s)
    (when-not (and (client? s) (not (root? s)))
      (throw (util/argument-error (str (name op) ": :sublevel must be a sublevel client")
                                  {:arg :sublevel :value s :op op})))
    (when-not (identical? (root s) (root c))
      (throw (util/argument-error (str (name op) ": :sublevel belongs to a different root database")
                                  {:arg :sublevel :op op})))
    s))

(defn- encode-op
  "Batch op map -> JS op object. Codec precedence: op-level :key-encoding /
   :value-encoding > the :sublevel client's codecs > `default-kc` /
   `default-vc` (batch-level override or the client's codecs)."
  [op-name c default-kc default-vc idx op]
  (let [op (util/normalize-op idx op)
        sub (op-sublevel! op-name c (:sublevel op))
        kc (cond (:key-encoding op) (codec/resolve (:key-encoding op))
                 sub (:key-codec sub)
                 :else default-kc)
        vc (cond (:value-encoding op) (codec/resolve (:value-encoding op))
                 sub (:value-codec sub)
                 :else default-vc)
        put? (= :put (:type op))
        m (cond-> {:type (name (:type op))
                   :key (codec/encode kc (:key op) :key)
                   :key-encoding (:upstream kc)}
            put? (assoc :value (codec/encode vc (:value op) :value)
                        :value-encoding (:upstream vc))
            sub (assoc :sublevel (js-db sub)))]
    (js-opts m)))

(defn batch!
  "Atomically applies a vector of operations:
   [{:type :put :key k :value v} {:type :del :key k}], each optionally with
   :key-encoding / :value-encoding (per-op codec) and :sublevel (a sublevel
   client of the same root: the op is prefixed and encoded for it; a
   sublevel of another database is an argument-error). opts: :key-encoding
   / :value-encoding (batch-level defaults), :sync. Every op is validated
   and encoded before upstream is called. Resolves nil."
  [c ops & [opts]]
  (run :batch
       (fn []
         (check-client! :batch c)
         (when-not (sequential? ops)
           (throw (util/argument-error "batch!: ops must be a sequential collection of op maps"
                                       {:arg :ops :value ops :op :batch})))
         (let [opts (opts! :batch batch-options opts)
               kc (key-codec c opts)
               vc (value-codec c opts)
               js-ops (to-array (map-indexed #(encode-op :batch c kc vc %1 %2) ops))
               jo (js-opts (dissoc opts :key-encoding :value-encoding))]
           (p/then (js-invoke (js-db c) "batch" js-ops jo) (constantly nil))))))

(defn clear!
  "Deletes every entry, or the range selected by :gt / :gte / :lt / :lte
   (encoded with the key codec), :limit (with :reverse: the LAST n),
   :key-encoding, :snapshot. NOT ATOMIC: a concurrent
   writer may interleave; iterate keys into a batch! when atomicity matters.
   Resolves nil."
  [c & [opts]]
  (run :clear
       (fn []
         (check-client! :clear c)
         (let [opts (->> (opts! :clear clear-options opts) (util/check-limit! :clear))
               kc (key-codec c opts)
               jo (js-opts (upstream-opts :clear (encode-bounds kc opts) kc nil))]
           (p/then (js-invoke (js-db c) "clear" jo) (constantly nil))))))

;; ---------------------------------------------------------------------------
;; sublevels
;; ---------------------------------------------------------------------------

(defn sublevel
  "A sublevel client for `name` (a string, or a vector of strings for a
   nested prefix with `c` as direct parent). Keys are namespaced under
   `separator + name + separator` (default separator \"!\"; names must be
   printable ASCII above '\"', else :code :invalid-prefix). opts:
   :separator, :key-encoding, :value-encoding — the codecs default to the
   PARENT'S (unlike upstream, which resets sublevels to utf8). Synchronous;
   throws normalized errors. The upstream handle is re-derived after the
   root is reopened, so a sublevel client survives close! + open! of its
   root."
  [c name & [opts]]
  (check-client! :sublevel c)
  (when-not (or (string? name) (and (vector? name) (seq name) (every? string? name)))
    (throw (util/argument-error "sublevel: name must be a string or a non-empty vector of strings"
                                {:arg :name :value name :op :sublevel})))
  (let [opts (opts! :sublevel sublevel-options opts)
        kc (key-codec c opts)
        vc (value-codec c opts)
        sub (->Client :sublevel (root c) c name opts kc vc (atom {}))]
    (run-sync :sublevel #(js-db sub))
    sub))

(defn prefix
  "The key prefix string of a sublevel client (\"!people!\"), \"\" for a root."
  [c]
  (check-client! :prefix c)
  (if (root? c) "" (run-sync :prefix #(js-prop (js-db c) "prefix"))))

(defn path
  "Vector of sublevel names from the root down to `c` ([] for a root);
   with `local?` true only the names relative to the parent."
  [c & [local?]]
  (check-client! :path c)
  (if (root? c)
    []
    (run-sync :path #(vec (array-seq (js-invoke (js-db c) "path" (boolean local?)))))))

;; ---------------------------------------------------------------------------
;; snapshots
;; ---------------------------------------------------------------------------

(defn snapshot
  "An explicit snapshot handle (synchronous). Pass it as the :snapshot
   option of get! / get-many! / has! / has-many! / iterators / clear! to
   read a consistent state. MUST be closed with close-snapshot! (or use
   with-snapshot!). Throws :code :not-supported where
   (:explicit-snapshots (supports c)) is false."
  [c]
  (check-client! :snapshot c)
  (run-sync :snapshot
            (fn []
              (track! c :snapshot (->Snapshot c (js-invoke (js-db c) "snapshot") (atom {:open? true}))))))

(defn close-snapshot!
  "Closes a snapshot handle (idempotent). Resolves nil."
  [s]
  (run :close-snapshot
       (fn []
         (when-not (instance? Snapshot s)
           (throw (util/argument-error "close-snapshot!: expected a snapshot handle"
                                       {:arg :snapshot :value s :op :close-snapshot})))
         (p/then (js-invoke (:js s) "close")
                 (fn [_]
                   (untrack! (:client s) :snapshot s)
                   (swap! (:state s) assoc :open? false)
                   nil)))))

(defn with-snapshot!
  "Takes a snapshot, calls (f snapshot) (may return a promise) and closes
   the snapshot in a finally. Resolves to f's result."
  [c f]
  (run :with-snapshot
       (fn []
         (let [s (snapshot c)]
           (-> (p/promise (p/do (f s)))
               (p/then (fn [v] (p/then (close-snapshot! s) (constantly v))))
               (p/catch (fn [e] (p/then (close-snapshot! s) (fn [_] (throw e))))))))))

;; ---------------------------------------------------------------------------
;; chained batch
;; ---------------------------------------------------------------------------

(defn chained-batch
  "A chained batch builder handle (synchronous; the client must be open —
   always true for clients from open!). Queue ops with batch-put! /
   batch-del!, drop them with batch-clear!, commit atomically with
   batch-write! or discard with batch-close!."
  [c]
  (check-client! :chained-batch c)
  (run-sync :chained-batch
            (fn []
              (track! c :batch (->ChainedBatch c (js-invoke (js-db c) "batch") (atom {:open? true}))))))

(defn- check-batch! [op b]
  (when-not (instance? ChainedBatch b)
    (throw (util/argument-error (str (name op) ": expected a chained batch handle")
                                {:arg :batch :value b :op op}))))

(defn batch-put!
  "Queues a put on a chained batch. Synchronous, returns the batch for
   chaining; throws normalized errors (argument/codec errors before
   upstream, :code :batch-not-open after write/close). opts:
   :key-encoding, :value-encoding, :sublevel (same-root sublevel client)."
  [b key value & [opts]]
  (run-sync :batch-put
            (fn []
              (check-batch! :batch-put b)
              (let [c (:client b)
                    opts (opts! :batch-put batch-op-options opts)
                    sub (op-sublevel! :batch-put c (:sublevel opts))
                    kc (cond (:key-encoding opts) (codec/resolve (:key-encoding opts))
                             sub (:key-codec sub)
                             :else (:key-codec c))
                    vc (cond (:value-encoding opts) (codec/resolve (:value-encoding opts))
                             sub (:value-codec sub)
                             :else (:value-codec c))
                    ek (codec/encode kc key :key)
                    ev (codec/encode vc value :value)
                    jo (js-opts (cond-> {:key-encoding (:upstream kc) :value-encoding (:upstream vc)}
                                  sub (assoc :sublevel (js-db sub))))]
                (js-invoke (:js b) "put" ek ev jo)
                b))))

(defn batch-del!
  "Queues a del on a chained batch. Synchronous, chainable. opts:
   :key-encoding, :sublevel."
  [b key & [opts]]
  (run-sync :batch-del
            (fn []
              (check-batch! :batch-del b)
              (let [c (:client b)
                    opts (opts! :batch-del #{:key-encoding :sublevel} opts)
                    sub (op-sublevel! :batch-del c (:sublevel opts))
                    kc (cond (:key-encoding opts) (codec/resolve (:key-encoding opts))
                             sub (:key-codec sub)
                             :else (:key-codec c))
                    ek (codec/encode kc key :key)
                    jo (js-opts (cond-> {:key-encoding (:upstream kc)}
                                  sub (assoc :sublevel (js-db sub))))]
                (js-invoke (:js b) "del" ek jo)
                b))))

(defn batch-clear!
  "Drops every queued op of a chained batch (synchronous, chainable)."
  [b]
  (run-sync :batch-clear
            (fn []
              (check-batch! :batch-clear b)
              (js-invoke (:js b) "clear")
              b)))

(defn batch-length
  "Number of ops queued on a chained batch (synchronous)."
  [b]
  (check-batch! :batch-length b)
  (js-prop (:js b) "length"))

(defn batch-write!
  "Commits a chained batch atomically and closes it. opts: :sync. Resolves nil."
  [b & [opts]]
  (run :batch-write
       (fn []
         (check-batch! :batch-write b)
         (let [jo (js-opts (opts! :batch-write write-options opts))]
           (-> (js-invoke (:js b) "write" jo)
               (p/finally (fn [_ _]
                            (untrack! (:client b) :batch b)
                            (swap! (:state b) assoc :open? false)))
               (p/then (constantly nil)))))))

(defn batch-close!
  "Discards a chained batch without writing (idempotent). Resolves nil."
  [b]
  (run :batch-close
       (fn []
         (check-batch! :batch-close b)
         (p/then (js-invoke (:js b) "close")
                 (fn [_]
                   (untrack! (:client b) :batch b)
                   (swap! (:state b) assoc :open? false)
                   nil)))))

(defn with-batch!
  "Creates a chained batch, calls (f batch) (may return a promise), then
   writes it; on any error the batch is closed (discarded) and the error
   rethrown. Resolves nil."
  [c f]
  (run :with-batch
       (fn []
         (let [b (chained-batch c)]
           (-> (p/promise (p/do (f b)))
               (p/then (fn [_] (batch-write! b)))
               (p/catch (fn [e]
                          (p/then (batch-close! b) (fn [_] (throw e))))))))))

;; ---------------------------------------------------------------------------
;; iterators — handles for leveldb-cljs.iter
;; ---------------------------------------------------------------------------

(defn- iterator-opts!
  [op c opts allowed]
  (let [opts (->> (opts! op allowed opts) (util/check-limit! op))
        kc (key-codec c opts)
        vc (value-codec c opts)]
    (when (and (:keys-only opts) (:values-only opts))
      (throw (util/argument-error (str (name op) ": :keys-only? and :values-only? are exclusive")
                                  {:arg :opts :op op})))
    [kc vc (-> (encode-bounds kc opts)
               (dissoc :keys-only :values-only)
               (cond->
                 (:keys-only opts) (assoc :values false)
                 (:values-only opts) (assoc :keys false)))]))

(defn- make-iterator [op c kind method allowed opts]
  (check-client! op c)
  (run-sync op
            (fn []
              (let [[kc vc uopts] (iterator-opts! op c opts allowed)
                    jo (js-opts (upstream-opts op uopts kc vc))
                    js-it (method (js-db c) jo)]
                (track! c :iterator (->Iterator c kind kc vc js-it (atom {:open? true})))))))

(defn iterator
  "An entry iterator handle (synchronous constructor; throws normalized
   errors, e.g. :code :database-not-open on a closed client). Entries are
   read with it-next! / it-nextv! / it-all! as {:key k :value v} maps.
   opts: :gt :gte :lt :lte (bounds, encoded with the key codec; nil bounds
   are dropped), :reverse, :limit (nil/-1 unlimited, 0 nothing), :keys-only?
   / :values-only? (omit the other slot), :key-encoding, :value-encoding,
   :snapshot, :fill-cache, :high-water-mark-bytes. Every read is async and
   one read may be in flight per iterator (:code :iterator-busy otherwise).
   Unclosed iterators pin snapshots: always it-close! (it-all! and the
   channels of leveldb-cljs.iter close for you)."
  [c & [opts]]
  (make-iterator :iterator c :entries #(js-invoke %1 "iterator" %2) iterator-options opts))

(defn key-iterator
  "Like iterator but yields keys only (upstream `db.keys()`)."
  [c & [opts]]
  (make-iterator :key-iterator c :keys #(js-invoke %1 "keys" %2)
                 (disj iterator-options :value-encoding :keys-only :values-only) opts))

(defn value-iterator
  "Like iterator but yields values only (upstream `db.values()`)."
  [c & [opts]]
  (make-iterator :value-iterator c :values #(js-invoke %1 "values" %2)
                 (disj iterator-options :keys-only :values-only) opts))

(defn- check-iterator! [op it]
  (when-not (instance? Iterator it)
    (throw (util/argument-error (str (name op) ": expected an iterator handle")
                                {:arg :iterator :value it :op op}))))

(defn- decode-item [it item]
  (case (:kind it)
    :entries (let [k (undefined->nil (aget item 0))
                   v (undefined->nil (aget item 1))]
               (cond-> {}
                 (some? k) (assoc :key (codec/decode (:key-codec it) k :key))
                 (some? v) (assoc :value (codec/decode (:value-codec it) v :value))))
    :keys (codec/decode (:key-codec it) item :key)
    :values (codec/decode (:value-codec it) item :value)))

(defn it-next!
  "Promise of the next item ({:key ..:value ..} / key / value depending on
   the iterator kind) or nil at the end (the iterator stays open)."
  [it]
  (run :it-next
       (fn []
         (check-iterator! :it-next it)
         (p/then (js-invoke (:js it) "next")
                 (fn [item] (when-not (undefined? item) (decode-item it item)))))))

(defn it-nextv!
  "Promise of a vector of up to `n` next items ([] at the end). Upstream
   may return fewer than n (highWaterMarkBytes)."
  [it n]
  (run :it-nextv
       (fn []
         (check-iterator! :it-nextv it)
         (when-not (and (integer? n) (pos? n))
           (throw (util/argument-error "it-nextv!: n must be a positive integer"
                                       {:arg :n :value n :op :it-nextv})))
         (p/then (js-invoke (:js it) "nextv" n)
                 (fn [items] (mapv #(decode-item it %) (array-seq items)))))))

(defn it-all!
  "Promise of a vector of all remaining items; closes the iterator."
  [it]
  (run :it-all
       (fn []
         (check-iterator! :it-all it)
         (-> (js-invoke (:js it) "all")
             (p/finally (fn [_ _]
                          (untrack! (:client it) :iterator it)
                          (swap! (:state it) assoc :open? false)))
             (p/then (fn [items] (mapv #(decode-item it %) (array-seq items))))))))

(defn it-seek!
  "Repositions the iterator so the next read yields keys >= target (<= with
   :reverse); a target outside the range options ends the iteration.
   Synchronous and lazy; THROWS :code :iterator-busy while a read is in
   flight (which is why the channel API of leveldb-cljs.iter does not offer
   seek). opts: :key-encoding."
  [it target & [opts]]
  (run-sync :it-seek
            (fn []
              (check-iterator! :it-seek it)
              (let [opts (opts! :it-seek seek-options opts)
                    kc (if-some [e (:key-encoding opts)] (codec/resolve e) (:key-codec it))
                    et (codec/encode kc target :key)]
                (js-invoke (:js it) "seek" et (js-opts {:key-encoding (:upstream kc)}))
                nil))))

(defn it-close!
  "Closes the iterator (idempotent; waits for an in-flight read). Resolves nil."
  [it]
  (run :it-close
       (fn []
         (check-iterator! :it-close it)
         (p/then (js-invoke (:js it) "close")
                 (fn [_]
                   (untrack! (:client it) :iterator it)
                   (swap! (:state it) assoc :open? false)
                   nil)))))

(defn it-count
  "Number of items the iterator has yielded so far (synchronous)."
  [it]
  (check-iterator! :it-count it)
  (js-prop (:js it) "count"))

(defn it-open?
  "False once the iterator was closed through it-close! / it-all!."
  [it]
  (check-iterator! :it-open? it)
  (boolean (:open? @(:state it))))

;; ---------------------------------------------------------------------------
;; convenience readers (all!/keys!/values!)
;; ---------------------------------------------------------------------------

(defn all!
  "Promise of a vector of {:key k :value v} maps for the range (iterator
   opts). Loads everything into memory: pass :limit or use
   leveldb-cljs.iter channels for large ranges."
  [c & [opts]]
  (run :all (fn [] (it-all! (iterator c opts)))))

(defn keys!
  "Promise of a vector of keys for the range (key-iterator opts)."
  [c & [opts]]
  (run :keys (fn [] (it-all! (key-iterator c opts)))))

(defn values!
  "Promise of a vector of values for the range (value-iterator opts)."
  [c & [opts]]
  (run :values (fn [] (it-all! (value-iterator c opts)))))

;; ---------------------------------------------------------------------------
;; events: the upstream EventEmitter surface as a core.async
;; channel. abstract-level 3 emits opening / open / closing / closed (no
;; args), write (an array of normalized ops: put / del / batch / chained
;; write) and clear (the options object); the legacy put / del / batch
;; events are gone upstream (they warn LEVEL_LEGACY and never fire), so
;; leveldb-cljs DERIVES :put / :del / :batch from `write`.
;; ---------------------------------------------------------------------------

(def event-names
  "Every :event keyword an events! channel can deliver."
  #{:opening :open :closing :closed :put :del :batch :clear})

(def events-options
  "Options of events!: :buffer (channel buffer size, default 32) and :only
   (a set of event-names to deliver; the others are dropped)."
  #{:buffer :only})

(def ^:private upstream-events ["opening" "open" "closing" "closed" "write" "clear"])

(defn- event-decode
  "Decodes a raw key / value / bound from an upstream event payload with the
   client's codec. Upstream hands back the ORIGINAL input of the write (a
   string for the utf8-format codecs, bytes for the buffer ones), except
   that writes made through a sublevel reach the parent already prefixed
   and encoded (bytes on memory-level, strings on classic-level): bytes
   are turned into text for a utf8-format codec so both
   backends report the same thing. Returns [value decode-error]; a decode
   failure (a sublevel's non-EDN text under an :edn root, for instance)
   yields the raw value plus the codec-error instead of killing the
   subscription."
  [codec raw role]
  (let [raw (undefined->nil raw)
        raw (if (and (= :utf8 (:format codec)) (codec/bytes? raw))
              (.toString (js/Buffer.from raw) "utf8")
              raw)]
    (try
      [(codec/decode codec raw role) nil]
      (catch :default e
        [raw e]))))

(defn- with-decoded
  "assoc's `k` (decoded) onto event map `m`, recording a decode failure
   under :decode-error."
  [m k codec raw role]
  (let [[v err] (event-decode codec raw role)]
    (cond-> (assoc m k v)
      (some? err) (assoc :decode-error err))))

(defn- write-op->map
  "One upstream write op -> {:type :put/:del :key k [:value v] [:sublevel true]}.
   Upstream nulls op.sublevel before emitting (abstract-level.js 'Only
   prefix once'), so a routed op is recognizable only by the presence of
   the property: :sublevel true, with the prefixed key."
  [c op]
  (let [t (keyword (js-prop op "type"))
        routed? (js-in "sublevel" op)
        m (cond-> {:type t}
            routed? (assoc :sublevel true))
        m (with-decoded m :key (:key-codec c) (js-prop op "key") :key)]
    (if (= :put t)
      (with-decoded m :value (:value-codec c) (js-prop op "value") :value)
      m)))

(defn- write->event
  "An upstream `write` payload (array of ops) -> one leveldb-cljs event: a
   single op becomes {:event :put/:del ...} (upstream cannot tell a one-op
   batch! from a put!), several become {:event :batch :ops [...]}."
  [c ops]
  (let [ms (mapv #(write-op->map c %) (array-seq ops))]
    (if (= 1 (count ms))
      (let [{:keys [type] :as m} (first ms)]
        (assoc (dissoc m :type) :event type))
      {:event :batch :ops ms})))

(defn- clear->event
  "An upstream `clear` payload (the verbatim options object) ->
   {:event :clear [:gt/:gte/:lt/:lte decoded bounds] [:limit] [:reverse]}."
  [c jo]
  (let [opts (util/->kebab-keys (js->clj (or jo #js {})))
        kc (:key-codec c)]
    (reduce (fn [m [k v]]
              (cond
                (contains? util/range-keys k) (with-decoded m k kc v :key)
                (contains? #{:limit :reverse} k) (assoc m k v)
                :else m))
            {:event :clear}
            opts)))

(defn- events-loop!
  "Drains `queue` (a PersistentQueue in an atom, filled synchronously by the
   upstream listeners so order is preserved) into `ch`. Cancel-poll
   pattern: while idle it re-checks every util/cancel-poll-ms whether the
   consumer closed `ch`; a parked put does the same through
   util/put-or-cancel!. `release!` removes the upstream listeners exactly
   once; :closed is always the last event delivered."
  [ch queue signal only release!]
  (go-loop []
    (if-some [ev (peek @queue)]
      (do (swap! queue pop)
          (let [deliver? (or (nil? only) (contains? only (:event ev)))
                closed-ev? (= :closed (:event ev))
                ok? (if deliver? (<! (util/put-or-cancel! ch ev)) true)]
            (cond
              (not ok?) (release!)
              closed-ev? (do (release!) (async/close! ch))
              :else (recur))))
      (do (alts! [signal (async/timeout util/cancel-poll-ms)])
          (if (util/chan-closed? ch)
            (release!)
            (recur))))))

(defn events!
  "A core.async channel of the client's upstream events, in
   emission order:
     {:event :opening} {:event :open} {:event :closing} {:event :closed}
     {:event :put :key k :value v [:sublevel true]}
     {:event :del :key k [:sublevel true]}
     {:event :batch :ops [{:type :put :key k :value v} {:type :del :key k} ..]}
     {:event :clear [:gt/:gte/:lt/:lte bound] [:limit n] [:reverse b]}
   :put / :del / :batch are derived from the upstream `write` event (the
   legacy put/del/batch events no longer exist upstream): a one-op write
   (put!, del!, a one-op batch!) is :put / :del, more ops are :batch.
   Keys, values and bounds are decoded with the CLIENT'S codecs (per-call
   codec overrides are not visible to the event side; a value written with
   an override arrives as the client's codec reads its stored text);
   :sublevel true marks an op routed with the :sublevel batch option (its
   :key is then the prefixed key; upstream drops the sublevel reference
   before emitting, so the prefix itself is only recoverable from the key).
   Writes through a sublevel client also reach the parent's channel with
   the prefixed key and the raw stored value; subscribe on
   the sublevel client to see its own keys. A decode failure adds
   :decode-error (the ex-info) and keeps the raw value.
   Subscribing on a closed root client is allowed: the channel then delivers
   :opening / :open when open! is called on it (close it yourself if the
   reopen never comes). The channel closes after {:event :closed} (close!
   of the client or its root), and closing it yourself (core.async/close!) unsubscribes: the
   listeners are removed at the next event, when a parked put notices, or
   within util/cancel-poll-ms while idle (open-resources counts live
   subscriptions). While a subscription is live a cancel-poll timer ticks,
   so close the channel or the client before expecting the process to
   exit. Events are queued without bound while the consumer lags (a write
   cannot be held back). A subscription does not survive a reopen.
   opts: :buffer (default 32), :only (set of event-names). Never throws:
   an argument-error arrives as the channel's only item, then it closes."
  [c & [opts]]
  (let [opts (util/normalize-opts opts)
        buffer (get opts :buffer 32)
        ch (async/chan (if (and (integer? buffer) (pos? buffer)) buffer 32))]
    (try
      (check-client! :events c)
      (util/check-opts! :events events-options opts)
      (when-not (and (integer? buffer) (pos? buffer))
        (throw (util/argument-error "events!: :buffer must be a positive integer"
                                    {:arg :buffer :value buffer :op :events})))
      (let [only (:only opts)
            _ (when (and (some? only) (not (and (set? only) (every? event-names only))))
                (throw (util/argument-error (str "events!: :only must be a set of " (pr-str (vec (sort event-names))))
                                            {:arg :only :value only :op :events})))
            db (run-sync :events #(js-db c))
            queue (atom cljs.core/PersistentQueue.EMPTY)
            signal (async/chan (async/dropping-buffer 1))
            released? (atom false)
            push! (fn [ev] (swap! queue conj ev) (async/offer! signal true))
            listeners (into {}
                            (map (fn [n]
                                   [n (case n
                                        "write" (fn [ops] (push! (write->event c ops)))
                                        "clear" (fn [jo] (push! (clear->event c jo)))
                                        (fn [] (push! {:event (keyword n)})))]))
                            upstream-events)
            release! (fn []
                       (when (compare-and-set! released? false true)
                         (doseq [[n f] listeners] (js-invoke db "off" n f))
                         (untrack! c :subscription ch)))]
        (doseq [[n f] listeners] (js-invoke db "on" n f))
        (track! c :subscription ch)
        (events-loop! ch queue signal only release!))
      (catch :default e
        (async/put! ch (->error :events e))
        (async/close! ch)))
    ch))

;; ---------------------------------------------------------------------------
;; bytes helpers: opaque byte values without interop
;; ---------------------------------------------------------------------------

(def bytes?
  "True for an opaque byte value (Buffer / Uint8Array)."
  codec/bytes?)

(defn- check-bytes! [op b]
  (when-not (codec/bytes? b)
    (throw (util/argument-error (str (name op) ": expected bytes (Buffer / Uint8Array)")
                                {:arg :bytes :value b :op op}))))

(defn- check-string! [op s]
  (when-not (string? s)
    (throw (util/argument-error (str (name op) ": expected a string")
                                {:arg :string :value s :op op}))))

(defn bytes->hex
  "Hex string of an opaque byte value."
  [b]
  (check-bytes! :bytes->hex b)
  (.toString (js/Buffer.from b) "hex"))

(defn hex->bytes
  "Opaque bytes from an even-length hex string (argument-error otherwise)."
  [s]
  (when-not (codec/hex-string? s)
    (throw (util/argument-error "hex->bytes: expected an even-length hex string"
                                {:arg :string :value s :op :hex->bytes})))
  (js/Buffer.from s "hex"))

(defn bytes->base64
  "Base64 string of an opaque byte value."
  [b]
  (check-bytes! :bytes->base64 b)
  (.toString (js/Buffer.from b) "base64"))

(defn base64->bytes
  "Opaque bytes from a base64 string."
  [s]
  (when-not (codec/base64-string? s)
    (throw (util/argument-error "base64->bytes: expected a base64 string"
                                {:arg :string :value s :op :base64->bytes})))
  (js/Buffer.from s "base64"))

(defn bytes->utf8
  "UTF-8 text of an opaque byte value."
  [b]
  (check-bytes! :bytes->utf8 b)
  (.toString (js/Buffer.from b) "utf8"))

(defn utf8->bytes
  "Opaque bytes of a string's UTF-8 encoding."
  [s]
  (check-string! :utf8->bytes s)
  (js/Buffer.from s "utf8"))

(defn bytes-length
  "Byte length of an opaque byte value."
  [b]
  (check-bytes! :bytes-length b)
  (js-prop b "length"))

(defn bytes=
  "True when two byte values hold the same bytes."
  [a b]
  (check-bytes! :bytes= a)
  (check-bytes! :bytes= b)
  (.equals (js/Buffer.from a) (js/Buffer.from b)))

;; ---------------------------------------------------------------------------
;; error helpers re-exported from util
;; ---------------------------------------------------------------------------

(def error-data
  "Structured data of a leveldb-cljs error, nil for anything else (util)."
  util/error-data)
(def error-type
  ":leveldb-cljs/db-error, :leveldb-cljs/argument-error, :leveldb-cljs/codec-error or nil (util)."
  util/error-type)
(def error-code
  "The kebab :code of a db-error (:database-not-open, :locked, ...) or nil (util)."
  util/error-code)
(def not-open?
  "True when the error reports use of a closed database/iterator/batch/snapshot (util)."
  util/not-open?)
