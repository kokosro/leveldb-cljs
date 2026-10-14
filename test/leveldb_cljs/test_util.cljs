(ns leveldb-cljs.test-util
  "Shared fixtures for the leveldb-cljs tests. Not a test namespace itself
   (the name does not end in -test). Provides an in-process memory-level
   client factory (no disk) and a temporary on-disk `level` factory
   (os.tmpdir + fs.mkdtemp, removed afterwards) so both backends are
   exercised; nothing touches the network. The Node `os` / `fs` / `path`
   calls below are the only JS interop outside leveldb-cljs.core and exist
   solely to create and remove temp directories for the tests."
  (:require [cljs.test :refer [is]]
            [promesa.core :as p]
            [leveldb-cljs.core :as core]))

(defn rejection
  "Promise of the rejection reason of `p` (fails the assertion if it resolves)."
  ;; NB: promesa treats a js/Error RETURNED from a catch handler as a
  ;; rejection (IPromiseFactory on js/Error), so the error must be wrapped in
  ;; an already-resolved promise to travel as a value.
  [promise]
  (-> promise
      (p/then (fn [v] (throw (ex-info "expected rejection" {:resolved v}))))
      (p/catch (fn [e] (p/resolved e)))))

(defn thrown
  "The value thrown by (f), nil when it returns normally."
  [f]
  (try (f) nil (catch :default e e)))

(defn finish
  "Runs promise `p` to completion inside an (async done ...) block: an
   unexpected rejection fails the test with its message; `done` is always
   called."
  [done p]
  (-> (p/promise p)
      (p/catch (fn [e]
                 (is false (str "unexpected rejection: " (ex-message e) " " (pr-str (ex-data e))))))
      (p/finally (fn [_ _] (done)))))

(defn sleep
  "Promise resolved after `ms` milliseconds."
  [ms]
  (p/delay ms nil))

;; ---------------------------------------------------------------------------
;; temp directories
;; ---------------------------------------------------------------------------

(def ^:private os (js/require "os"))
(def ^:private fs (js/require "fs"))
(def ^:private node-path (js/require "path"))

(defn tmp-dir!
  "Creates a fresh temporary directory under the OS temp dir (or
   $CLAUDE_JOB_DIR/tmp when set) and returns its path."
  []
  (let [base (or (some-> (unchecked-get js/process.env "CLAUDE_JOB_DIR") (str "/tmp"))
                 (.tmpdir os))]
    (.mkdtempSync fs (.join node-path base "leveldb-cljs-test-"))))

(defn rm-dir!
  "Removes a directory recursively (ignores a missing one)."
  [dir]
  (.rmSync fs dir #js {:recursive true :force true}))

(defn dir-files
  "Sorted vector of the file names inside `dir`."
  [dir]
  (vec (sort (array-seq (.readdirSync fs dir)))))

(defn with-tmp-dir
  "Calls (f dir) with a fresh temp directory; removes it afterwards.
   Returns the promise of f's result."
  [f]
  (let [dir (tmp-dir!)]
    (-> (p/promise (p/do (f dir)))
        (p/finally (fn [_ _] (rm-dir! dir))))))

;; ---------------------------------------------------------------------------
;; clients
;; ---------------------------------------------------------------------------

(defn with-memory
  "Opens a memory-level client with `opts` (default codecs utf8), calls
   (f client), closes the client afterwards. Returns f's promise."
  ([f] (with-memory {} f))
  ([opts f]
   (p/let [c (core/open! (assoc opts :memory? true))]
     (-> (p/promise (p/do (f c)))
         (p/finally (fn [_ _] (core/close! c)))))))

(defn with-tmp-db
  "Opens an on-disk `level` client in a fresh temp directory, calls
   (f client), closes the client and removes the directory afterwards."
  ([f] (with-tmp-db {} f))
  ([opts f]
   (with-tmp-dir
     (fn [dir]
       (p/let [c (core/open! (assoc opts :path (.join node-path dir "db")))]
         (-> (p/promise (p/do (f c)))
             (p/finally (fn [_ _] (core/close! c)))))))))

(defn with-each-backend
  "Runs (f client backend-keyword) first on memory-level, then on a temp
   on-disk database. Sequential; returns a promise."
  ([f] (with-each-backend {} f))
  ([opts f]
   (p/let [_ (with-memory opts #(f % :memory))
           _ (with-tmp-db opts #(f % :level))]
     nil)))

(defn seed!
  "Puts every [k v] pair of `kvs` (a map or seq of pairs) with batch!."
  [c kvs]
  (core/batch! c (mapv (fn [[k v]] {:type :put :key k :value v}) kvs)))
