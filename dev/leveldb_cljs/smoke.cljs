(ns leveldb-cljs.smoke
  "Live smoke: a real on-disk `level` (classic-level) database in a
   fresh directory under the OS temp dir, opened side by side with a
   memory-level one, driven through the public API only. The filesystem is
   the live part: nothing here touches the network. Not part of
   run-tests.sh.

     npx shadow-cljs compile smoke && node target/smoke.js

   Structure (house pattern): numbered sections that never abort early.
   Each prints `PASS <name> — <detail>` or `FAIL <name>: <reason>`, a tally
   comes last, the temp directory is removed in every case and the exit
   code is 0 only when every section passed.

   Interop note: the only JavaScript interop outside leveldb-cljs.core in
   this repo is here and in test-util, limited to creating / removing the
   temp directory (os, fs, path), reading process.platform / arch /
   versions for the platform report (inventory 9.10) and process.exit."
  (:require [cljs.core.async :as async]
            [clojure.string :as str]
            [leveldb-cljs.core :as core]
            [leveldb-cljs.iter :as iter]
            [leveldb-cljs.util :as util]
            [promesa.core :as p]))

;; ---------------------------------------------------------------------------
;; Bookkeeping
;; ---------------------------------------------------------------------------

(def results
  "[{:name .. :ok? .. :reason ..} ...] in section order."
  (atom []))

(def shared
  "Cross-section values: :dir (temp directory), :db (on-disk client),
   :mem (memory client), :path."
  (atom {}))

(defn say
  "println of the joined arguments."
  [& xs]
  (println (str/join " " (map str xs))))

(defn error-brief
  "One line describing a rejection: the leveldb-cljs error type, code,
   reason, op and message."
  [e]
  (let [d (or (util/error-data e) (ex-data e))]
    (if d
      (str (name (or (:type d) :error))
           (when (:code d) (str " code=" (:code d)))
           (when (:reason d) (str " reason=" (:reason d)))
           (when (:op d) (str " op=" (:op d)))
           (when (:cause-code d) (str " cause-code=" (:cause-code d)))
           " — " (or (:message d) (ex-message e)))
      (or (ex-message e) (str e)))))

(defn section!
  "Runs `f` (a fn returning a promise of a detail string, or throwing /
   rejecting) as section `name`. Prints PASS/FAIL, records the result and
   ALWAYS resolves so the next section runs."
  [name f]
  (-> (p/do (f))
      (p/then (fn [detail]
                (say (str "PASS " name (when detail (str " — " detail))))
                (swap! results conj {:name name :ok? true})))
      (p/catch (fn [e]
                 (let [reason (error-brief e)]
                   (say (str "FAIL " name ": " reason))
                   (swap! results conj {:name name :ok? false :reason reason}))))))

(defn fail!
  "Throws a plain smoke failure with `reason`."
  [reason & [data]]
  (throw (ex-info reason (merge {:type :smoke/fail} data))))

(defn expect!
  "fail! with `what` unless `ok?`."
  [ok? what]
  (when-not ok? (fail! what)))

(defn rejection
  "Promise of the rejection reason of `prom`; fails when it resolves."
  [prom]
  (-> prom
      (p/then (fn [v] (fail! (str "expected a rejection, resolved " (pr-str v)))))
      (p/catch (fn [e] (p/resolved e)))))

(defn thrown
  "The value thrown by (f), nil when it returns normally."
  [f]
  (try (f) nil (catch :default e e)))

;; ---------------------------------------------------------------------------
;; Temp directory and platform report (the only interop in this file)
;; ---------------------------------------------------------------------------

(def ^:private os (js/require "os"))
(def ^:private fs (js/require "fs"))
(def ^:private node-path (js/require "path"))

(defn tmp-dir!
  "A fresh directory under the OS temp dir (os.tmpdir + mkdtemp)."
  []
  (.mkdtempSync fs (.join node-path (.tmpdir os) "leveldb-cljs-smoke-")))

(defn rm-dir!
  "Removes a directory recursively (missing is fine)."
  [dir]
  (.rmSync fs dir #js {:recursive true :force true}))

(defn dir-files
  "Sorted vector of the file names inside `dir`."
  [dir]
  (vec (sort (array-seq (.readdirSync fs dir)))))

(defn platform-report
  "Inventory 9.10: process.platform / arch / Node version, the `level` and
   `classic-level` versions, whether the matching prebuild directory is
   present and whether a local compile happened (a `build/` directory
   under classic-level)."
  []
  (let [platform (.-platform js/process)
        arch (.-arch js/process)
        node (.-node (.-versions js/process))
        cl-dir (.dirname node-path (js/require.resolve "classic-level/package.json"))
        prebuild (.join node-path cl-dir "prebuilds" (str platform "-" arch))
        prebuild? (.existsSync fs prebuild)
        compiled? (.existsSync fs (.join node-path cl-dir "build"))]
    {:platform platform :arch arch :node node
     :level (.-version (js/require "level/package.json"))
     :classic-level (.-version (js/require "classic-level/package.json"))
     :prebuild-dir (when prebuild? (vec (sort (array-seq (.readdirSync fs prebuild)))))
     :backend (cond compiled? :compiled-locally prebuild? :prebuilt :else :unknown)}))

;; ---------------------------------------------------------------------------
;; Sections
;; ---------------------------------------------------------------------------

(defn s0-open! []
  (let [dir (tmp-dir!)
        path (.join node-path dir "db")
        report (platform-report)]
    (swap! shared assoc :dir dir :path path)
    (p/let [db (core/open! {:path path :value-encoding :utf8})
            mem (core/open! {:memory? true})
            _ (swap! shared assoc :db db :mem mem)
            locked (rejection (core/open! path))
            _ (core/put! db "probe" "fs")
            _ (core/put! mem "probe" "mem")
            v-fs (core/get! db "probe")
            v-mem (core/get! mem "probe")]
      (expect! (= :open (core/status db)) (str "fs status " (core/status db)))
      (expect! (= :open (core/status mem)) (str "memory status " (core/status mem)))
      (expect! (= :level (core/backend db)) "fs backend keyword")
      (expect! (= :memory (core/backend mem)) "memory backend keyword")
      (expect! (= path (core/location db)) "location")
      (expect! (= :database-not-open (util/error-code locked))
               (str "second open of the same directory: " (error-brief locked)))
      (expect! (= :locked (:reason (ex-data locked)))
               (str "second open should report :reason :locked, got " (error-brief locked)))
      (expect! (and (= "fs" v-fs) (= "mem" v-mem)) "side-by-side put/get")
      (expect! (some #(str/starts-with? % "MANIFEST") (dir-files path)) "no MANIFEST in the db dir")
      (expect! (every? #(get (:events (core/supports db)) %) [:opening :open :closing :closed :write :clear])
               (str "supports.events " (pr-str (:events (core/supports db)))))
      (str "platform=" (:platform report) "/" (:arch report) " node=" (:node report)
           " level=" (:level report) " classic-level=" (:classic-level report)
           " native=" (name (:backend report)) " prebuild=" (pr-str (:prebuild-dir report))
           " | fs " path " files=" (pr-str (dir-files path))
           " | second open: " (error-brief locked)
           " | memory-level side by side, both :open, supports.events all six"))))

(defn s1-round-trips! []
  (let [{:keys [db]} @shared
        hex-value "cafe00"]
    (p/let [_ (core/put! db "u" "utf8 ✓ text")
            _ (core/put! db "j" {:n 1 :s "x" :v [1 2 {:k true}]} {:value-encoding :json})
            _ (core/put! db "e" {:kw :a :set #{1 2} :sym 'q :nested {:v [1.5 nil]}} {:value-encoding :edn})
            _ (core/put! db "r" "raw text" {:value-encoding :raw})
            _ (core/put! db "h" hex-value {:value-encoding :hex})
            u (core/get! db "u")
            j (core/get! db "j" {:value-encoding :json})
            e (core/get! db "e" {:value-encoding :edn})
            r (core/get! db "r" {:value-encoding :raw})
            h (core/get! db "h" {:value-encoding :hex})
            h-raw (core/get! db "h" {:value-encoding :raw})
            has-u (core/has! db "u")
            has-none (core/has! db "nope")
            many (core/get-many! db ["u" "nope" "r"] {:value-encoding :raw})
            sync-u (core/get-sync db "u")
            sync-e (core/get-sync db "e" {:value-encoding :edn})
            _ (core/del! db "u")
            after-del (core/get! db "u")
            _ (core/del! db "never-there")]
      (expect! (= "utf8 ✓ text" u) (str "utf8 " (pr-str u)))
      (expect! (= {:n 1 :s "x" :v [1 2 {:k true}]} j) (str "json " (pr-str j)))
      (expect! (= {:kw :a :set #{1 2} :sym 'q :nested {:v [1.5 nil]}} e) (str "edn " (pr-str e)))
      (expect! (and (core/bytes? r) (= "raw text" (core/bytes->utf8 r))) "raw read back as bytes")
      (expect! (= hex-value h) (str "hex " (pr-str h)))
      (expect! (= hex-value (core/bytes->hex h-raw)) "hex stored as 3 bytes")
      (expect! (= 3 (core/bytes-length h-raw)) "hex byte length")
      (expect! (and has-u (false? has-none)) "has!")
      (expect! (= [true false true] (mapv some? many)) "get-many! nil at the absent slot")
      (expect! (and (= "utf8 ✓ text" sync-u) (= e sync-e)) "get-sync")
      (expect! (nil? after-del) "del! then get! -> nil")
      (str "utf8/json/edn/raw/hex values round-trip; has!=" has-u "/" has-none
           " get-many!=" (pr-str (mapv #(when % (core/bytes->utf8 %)) many))
           " get-sync ok; del! of a present and of an absent key resolve; get! after del -> nil"))))

(defn s2-batches! []
  (let [{:keys [db]} @shared
        people (core/sublevel db "people" {:value-encoding :edn})]
    (p/let [_ (core/batch! db [{:type :put :key "b1" :value "one"}
                               {:type :put :key "b2" :value "two"}
                               {:type :del :key "b1"}
                               {:type :put :key "alice" :value {:age 30} :sublevel people}])
            b (core/chained-batch db)
            _ (-> b
                  (core/batch-put! "c1" "chained")
                  (core/batch-put! "bob" {:age 41} {:sublevel people})
                  (core/batch-del! "b2"))
            len (core/batch-length b)
            _ (core/batch-write! b)
            after-write (thrown #(core/batch-put! b "x" "y"))
            b1 (core/get! db "b1")
            b2 (core/get! db "b2")
            c1 (core/get! db "c1")
            alice (core/get! people "alice")
            bob (core/get! people "bob")
            raw-alice (core/get! db "!people!alice")
            _ (core/with-batch! db (fn [wb] (core/batch-put! wb "w1" "with") (core/batch-put! wb "w2" "batch")))
            w (core/get-many! db ["w1" "w2"])
            discarded (core/chained-batch db)
            _ (core/batch-put! discarded "never" "written")
            _ (core/batch-close! discarded)
            never (core/get! db "never")
            res (core/open-resources db)]
      (expect! (nil? b1) "array batch: the del after the put of the same key wins")
      (expect! (nil? b2) "chained batch: del applied")
      (expect! (= "chained" c1) (str "chained put " (pr-str c1)))
      (expect! (= 3 len) (str "batch-length " len))
      (expect! (= :batch-not-open (util/error-code after-write))
               (str "batch-put! after write: " (some-> after-write error-brief)))
      (expect! (= {:age 30} alice) (str "array batch :sublevel op " (pr-str alice)))
      (expect! (= {:age 41} bob) (str "chained batch :sublevel op " (pr-str bob)))
      (expect! (= "{:age 30}" raw-alice) (str "the root sees the prefixed key and the EDN text " (pr-str raw-alice)))
      (expect! (= ["with" "batch"] w) "with-batch! wrote both")
      (expect! (nil? never) "batch-close! discarded the op")
      (expect! (= 0 (:batches res)) (str "open batches " (:batches res)))
      (str "array batch (put/put/del + :sublevel op) and chained batch (" len " ops incl. :sublevel) atomic;"
           " root reads " (pr-str raw-alice) " under !people!alice; with-batch! ok; batch-close! discards;"
           " batch-put! after write -> " (util/error-code after-write)))))

(defn- iterator-count-after
  "Promise of (:iterators (open-resources db)) after `ms`."
  [db ms]
  (p/let [_ (p/delay ms nil)]
    (:iterators (core/open-resources db))))

(defn s3-iterators! []
  (let [{:keys [db]} @shared
        it (core/sublevel db "it")]
    (p/let [_ (core/batch! it (concat [{:type :put :key "" :value "empty"}]
                                      (for [i (range 200)]
                                        {:type :put :key (str "k" (.padStart (str i) 3 "0")) :value (str "v" i)})))
            all (iter/entries-seq! it)
            gte (iter/keys-seq! it {:gte "k198"})
            gt (iter/keys-seq! it {:gt "k198"})
            lte (iter/keys-seq! it {:lte "k001"})
            lt (iter/keys-seq! it {:lt "k001"})
            lte-empty (iter/keys-seq! it {:lte ""})
            gt-empty-limit (iter/keys-seq! it {:gt "" :limit 2})
            rev (iter/keys-seq! it {:reverse true :limit 3})
            ko (iter/entries-seq! it {:keys-only? true :limit 2})
            vo (iter/entries-seq! it {:values-only? true :limit 2})
            vs (iter/values-seq! it {:gte "k198"})
            core-keys (core/keys! it {:gte "k199"})
            ch (iter/entries! it {:buffer 1 :batch-size 10})
            first-item (p/create (fn [resolve _] (async/take! ch resolve)))
            during (:iterators (core/open-resources db))
            _ (async/close! ch)
            after (iterator-count-after db (* 3 util/cancel-poll-ms))
            reduced-sum (iter/reduce! (fn [acc {:keys [key]}] (if (= key "k010") (reduced (inc acc)) (inc acc))) 0 (iter/entries! it))
            after-reduce (iterator-count-after db (* 3 util/cancel-poll-ms))]
      (expect! (= 201 (count all)) (str "full drain " (count all)))
      (expect! (= {:key "" :value "empty"} (first all)) "the empty-string key sorts first")
      (expect! (= ["k198" "k199"] gte) (str ":gte " (pr-str gte)))
      (expect! (= ["k199"] gt) (str ":gt " (pr-str gt)))
      (expect! (= ["" "k000" "k001"] lte) (str ":lte " (pr-str lte)))
      (expect! (= ["" "k000"] lt) (str ":lt " (pr-str lt)))
      (expect! (= [""] lte-empty) (str ":lte \"\" " (pr-str lte-empty)))
      (expect! (= ["k000" "k001"] gt-empty-limit) (str ":gt \"\" :limit 2 " (pr-str gt-empty-limit)))
      (expect! (= ["k199" "k198" "k197"] rev) (str ":reverse :limit 3 " (pr-str rev)))
      (expect! (= [{:key ""} {:key "k000"}] ko) (str ":keys-only? " (pr-str ko)))
      (expect! (= [{:value "empty"} {:value "v0"}] vo) (str ":values-only? " (pr-str vo)))
      (expect! (= ["v198" "v199"] vs) (str "values! " (pr-str vs)))
      (expect! (= ["k199"] core-keys) "core/keys!")
      (expect! (= {:key "" :value "empty"} first-item) "first channel item")
      (expect! (= 1 during) (str "iterator open while the channel is live: " during))
      (expect! (= 0 after) (str "iterator still open after channel close: " after))
      (expect! (= 12 reduced-sum) (str "reduce! early stop (\"\" + k000..k010) " reduced-sum))
      (expect! (= 0 after-reduce) "reduce! (reduced) released the iterator")
      (str "201 entries drained; gte/gt/lte/lt incl. the empty-string key; reverse+limit;"
           " keys-only/values-only; channel close released the iterator (" during " -> " after ");"
           " reduce! early stop released it too"))))

(defn s4-sublevels! []
  (let [{:keys [db mem]} @shared
        a (core/sublevel db "a")
        b (core/sublevel db "b")
        nested (core/sublevel a "inner")
        foreign (core/sublevel mem "foreign")]
    (p/let [_ (core/put! a "k" "from a")
            _ (core/put! b "k" "from b")
            _ (core/put! nested "k" "from a/inner")
            va (core/get! a "k")
            vb (core/get! b "k")
            vn (core/get! nested "k")
            a-keys (iter/keys-seq! a)
            root-keys (iter/keys-seq! db {:gte "!a!" :lt "!b!"})
            cross (rejection (core/batch! db [{:type :put :key "x" :value "y" :sublevel foreign}]))
            cross-chained (thrown #(core/batch-put! (core/chained-batch db) "x" "y" {:sublevel foreign}))
            _ (core/close! db)
            closed-status (core/status a)
            closed-get (rejection (core/get! a "k"))
            _ (core/open! db)
            va2 (core/get! a "k")
            vn2 (core/get! nested "k")
            bad-name (thrown #(core/sublevel db "bad name!"))]
      (expect! (and (= "from a" va) (= "from b" vb) (= "from a/inner" vn)) "isolation")
      (expect! (= ["!inner!k" "k"] a-keys) (str "sublevel a sees its own keys + nested prefix " (pr-str a-keys)))
      (expect! (= ["!a!!inner!k" "!a!k"] root-keys) (str "root range " (pr-str root-keys)))
      (expect! (= :leveldb-cljs/argument-error (util/error-type cross)) (str "cross-root batch! " (error-brief cross)))
      (expect! (= :leveldb-cljs/argument-error (util/error-type cross-chained)) "cross-root batch-put!")
      (expect! (= :closed closed-status) (str "sublevel status after root close " closed-status))
      (expect! (= :database-not-open (util/error-code closed-get)) (str "get on closed sublevel " (error-brief closed-get)))
      (expect! (and (= "from a" va2) (= "from a/inner" vn2)) "sublevel clients re-derive after reopen")
      (expect! (= :invalid-prefix (util/error-code bad-name)) (str "bad name " (some-> bad-name error-brief)))
      (str "a/b/a!inner isolated; prefixes " (core/prefix a) " " (core/prefix nested) " path=" (pr-str (core/path nested))
           "; foreign-root sublevel rejected (batch! + batch-put!); after close!/open! the same sublevel clients read again;"
           " invalid name -> " (util/error-code bad-name)))))

(defn s5-clear-and-persistence! []
  (let [{:keys [db path]} @shared
        pers (core/sublevel db "persist" {:value-encoding :edn})
        transitions (atom [])]
    (p/let [_ (core/batch! pers (for [i (range 10)] {:type :put :key (str "p" i) :value {:i i}}))
            _ (core/clear! pers {:gte "p5"})
            left (iter/keys-seq! pers)
            _ (core/clear! db {:gte "!it!" :lt "!it\""})
            it-left (iter/keys-seq! db {:gte "!it!" :lt "!it\""})
            ch (core/events! db {:only #{:closing :closed :opening :open}})
            _ (core/close! db)
            _ (swap! transitions conj (core/status db))
            reopened (core/open! path)
            _ (swap! transitions conj (core/status reopened))
            files (dir-files path)
            v3 (core/get! reopened "!persist!p3")
            _ (core/close! reopened)
            _ (core/open! db)
            _ (swap! transitions conj (core/status db))
            v3-edn (core/get! pers "p3")
            lifecycle (p/create (fn [resolve _]
                                  (async/go (resolve [(async/<! ch) (async/<! ch) (async/<! ch)]))))
            _ (async/close! ch)]
      (expect! (= ["p0" "p1" "p2" "p3" "p4"] left) (str "clear! range " (pr-str left)))
      (expect! (empty? it-left) (str "clear! of the it sublevel range " (count it-left)))
      (expect! (= [:closed :open :open] @transitions) (str "status transitions " (pr-str @transitions)))
      (expect! (= "{:i 3}" v3) (str "a fresh client on the same path reads the EDN text " (pr-str v3)))
      (expect! (= {:i 3} v3-edn) (str "after close!/open! the sublevel decodes " (pr-str v3-edn)))
      (expect! (= [{:event :closing} {:event :closed}] (take 2 lifecycle)) (str "lifecycle events " (pr-str lifecycle)))
      (expect! (nil? (nth lifecycle 2)) "the events channel closed after :closed")
      (str "clear! :gte kept " (pr-str left) "; range clear emptied !it!; close!/open! transitions " (pr-str @transitions)
           "; values survived close (fresh client on the same path + reopened client); files=" (pr-str files)))))

(defn s6-events! []
  (let [{:keys [db]} @shared
        ch (core/events! db {:only #{:put :del :batch :clear}})]
    (p/let [_ (core/put! db "ev1" "one")
            _ (core/del! db "ev1")
            _ (core/batch! db [{:type :put :key "ev2" :value "two"} {:type :del :key "ev2"}])
            _ (core/clear! db {:gte "ev" :lt "ew"})
            evs (p/create (fn [resolve _]
                            (async/go (resolve [(async/<! ch) (async/<! ch) (async/<! ch) (async/<! ch)]))))
            during (:subscriptions (core/open-resources db))
            _ (async/close! ch)
            _ (p/delay (* 3 util/cancel-poll-ms) nil)
            after (:subscriptions (core/open-resources db))]
      (expect! (= [{:event :put :key "ev1" :value "one"}
                   {:event :del :key "ev1"}
                   {:event :batch :ops [{:type :put :key "ev2" :value "two"} {:type :del :key "ev2"}]}
                   {:event :clear :gte "ev" :lt "ew"}]
                  evs)
               (str "events " (pr-str evs)))
      (expect! (= 1 during) (str "subscriptions while live " during))
      (expect! (= 0 after) (str "subscriptions after channel close " after))
      (str "put/del/batch/clear observed in order on the on-disk db: " (pr-str (mapv :event evs))
           "; closing the channel released the listeners (" during " -> " after ")"))))

(defn s7-codec-edges! []
  (let [{:keys [db]} @shared]
    (p/let [guard (rejection (core/put! db "map" {:a 1}))
            guard-key (rejection (core/put! db {:k 1} "v"))
            nil-value (rejection (core/put! db "k" nil))
            missing (core/get! db "definitely-not-there")
            missing-sync (core/get-sync db "definitely-not-there")
            bad-hex (rejection (core/put! db "h" "xyz" {:value-encoding :hex}))
            unknown-codec (rejection (core/get! db "k" {:value-encoding :nope}))
            unknown-opt (rejection (core/get! db "k" {:fillCache true}))]
      (expect! (= :leveldb-cljs/codec-error (util/error-type guard)) (str "utf8 map value " (error-brief guard)))
      (expect! (= :leveldb-cljs/codec-error (util/error-type guard-key)) (str "utf8 map key " (error-brief guard-key)))
      (expect! (= :leveldb-cljs/argument-error (util/error-type nil-value)) (str "nil value " (error-brief nil-value)))
      (expect! (nil? missing) (str "get! of a missing key " (pr-str missing)))
      (expect! (nil? missing-sync) (str "get-sync of a missing key " (pr-str missing-sync)))
      (expect! (= :leveldb-cljs/codec-error (util/error-type bad-hex)) (str "bad hex " (error-brief bad-hex)))
      (expect! (= :leveldb-cljs/argument-error (util/error-type unknown-codec)) "unknown codec")
      (expect! (= :leveldb-cljs/argument-error (util/error-type unknown-opt)) "camelCase option key rejected")
      (str "silent-stringify guard: " (error-brief guard)
           " | nil value -> argument-error | missing key -> nil (get! and get-sync)"
           " | bad hex -> codec-error | unknown codec / camelCase option -> argument-error"))))

;; ---------------------------------------------------------------------------
;; Main
;; ---------------------------------------------------------------------------

(defn- cleanup! []
  (let [{:keys [db mem dir]} @shared]
    (-> (p/all [(if db (core/close! db) (p/resolved nil))
                (if mem (core/close! mem) (p/resolved nil))])
        (p/catch (fn [e] (say "cleanup close failed:" (error-brief e))))
        (p/finally (fn [_ _]
                     (when dir
                       (rm-dir! dir)
                       (say (str "removed " dir " (exists after rm: " (.existsSync fs dir) ")"))))))))

(defn main [& _]
  (say "leveldb-cljs LIVE smoke —" (js/Date.) "— real on-disk classic-level db under" (.tmpdir os) "+ memory-level; no network")
  (-> (p/do
        (section! "0 open! on-disk + memory side by side, platform report, lock" s0-open!)
        (section! "1 put!/get!/del!/has!/get-sync round-trips (utf8 json edn raw hex)" s1-round-trips!)
        (section! "2 batch! array + chained batch incl. :sublevel ops" s2-batches!)
        (section! "3 iterators: channels, ranges, reverse/limit, only-flags, cancel releases" s3-iterators!)
        (section! "4 sublevels: isolation, reopen re-derive, cross-root rejection" s4-sublevels!)
        (section! "5 clear! + persistence across close!/open!, status transitions" s5-clear-and-persistence!)
        (section! "6 events! channel live" s6-events!)
        (section! "7 codec edges: silent-stringify guard, not-found nil" s7-codec-edges!))
      (p/catch (fn [e] (say "FAIL harness:" (error-brief e))))
      (p/then cleanup!)
      (p/finally (fn [_ _]
                   (let [rs @results
                         failed (remove :ok? rs)]
                     (say "")
                     (say (str "TALLY: " (count (filter :ok? rs)) "/" (count rs) " sections passed"))
                     (doseq [f failed] (say (str "  FAILED " (:name f) ": " (:reason f))))
                     (js/process.exit (if (and (seq rs) (empty? failed)) 0 1)))))))
