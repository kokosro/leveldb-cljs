# leveldb-cljs


`leveldb-cljs` is a client for LevelDB on Node.js, built on the
`level` npm package, a Node-API binding of LevelDB 1.20. LevelDB is a single-process key-value
store: a directory on disk, keys kept in sorted order, atomic batches, range scans and
snapshots, with no server to run. The library exposes it as plain ClojureScript: values
are ClojureScript data under a codec of your choice, options and results are kebab-case
keyword maps, every I/O operation returns a promesa
promise, and iterators and change events are `core.async` channels. It needs no network.
`level` is the only runtime npm dependency.

- [Quick start](#quick-start)
- [What to use it for](#what-to-use-it-for)
- [Sizes are yours](#sizes-are-yours)
- [Features](#features)
- [Error handling](#error-handling)
- [Reference](#reference)
- [Development](#development)

## Quick start

Open a database in a fresh directory, put and get a value, write a batch, read ranges,
close, open again and find the data still there. You need Node.js 18 or later and a JDK
(for shadow-cljs).

**1. Get the code.** `level` itself is an npm package that
your project installs; it ships prebuilt binaries for the common platforms.

```sh

mkdir -p my-app/src/quickstart && cd my-app
npm init -y
npm install level
npm install --save-dev shadow-cljs
```

**2. Declare the dependency.** With the Clojure CLI, `deps.edn` names the checkout and
`shadow-cljs.edn` defers to it:

```clojure
;; deps.edn
{:paths ["src"]
 :deps {kokosro/leveldb-cljs {:local/root "../leveldb-cljs"}
        thheller/shadow-cljs {:mvn/version "2.28.23"}}}
```

```clojure
;; shadow-cljs.edn
{:deps true
 :builds {:app {:target :node-script
                :main quickstart.core/main
                :output-to "out/quickstart.js"}}}
```

Without the Clojure CLI, leave `deps.edn` out and put the sources on the path of
shadow-cljs:

```clojure
;; shadow-cljs.edn
{:source-paths ["src" "../leveldb-cljs/src"]
 :dependencies [[funcool/promesa "12.0.1"]]
 :builds {:app {:target :node-script
                :main quickstart.core/main
                :output-to "out/quickstart.js"}}}
```

**3. Write the program.** `src/quickstart/core.cljs`:

```clojure
(ns quickstart.core
  (:require ["fs" :as fs]
            ["os" :as os]
            ["path" :as path]
            [leveldb-cljs.core :as db]
            [promesa.core :as p]))

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
```

**4. Run it.**

```sh
npx shadow-cljs compile app
node out/quickstart.js
```

It prints the following, and removes the directory:

```clojure
:alice {:name "Alice", :score 620}
:nobody nil
:present [false true]
:all-keys ["user:bob" "user:carol" "visit:1"]
:users [{:key "user:bob", :value {:name "Bob", :score 10}} {:key "user:carol", :value {:name "Carol", :score 75}}]
:last-one [{:key "visit:1", :value {:user "bob"}}]
:reopened {:name "Carol", :score 75}
```

| Line | What it is |
|------|------------|
| `:alice` | the value under `"user:alice"`, read back as the map that was put: the `:edn` codec stores it as text and reads it again |
| `:nobody` | a key that is absent is `nil`. It is not an error |
| `:present` | after the batch: `"user:alice"` was deleted and `"user:bob"` was put, in one atomic write |
| `:all-keys` | every key, in sorted order |
| `:users` | the range from `"user:"` up to `"user;"`, the character after the colon: every key with the prefix `user:` |
| `:last-one` | the range in reverse with a limit of 1: the last entry |
| `:reopened` | read after `close!` and a second `open!` of the directory: the data is on disk |

The checkout runs the same program without a project of your own:

```sh
cd leveldb-cljs
npm install
npx shadow-cljs compile examples && node target/examples.js
```

## What to use it for

The examples take an open client `c` and these namespaces:

```clojure
(ns my.app
  (:require [cljs.core.async :as async]
            [leveldb-cljs.core :as db]
            [leveldb-cljs.iter :as iter]
            [promesa.core :as p]))

(p/let [c (db/open! {:path "data" :value-encoding :edn})]
  ...)
```

### Records and an index, written together

A sublevel is a part of the database under a key prefix, with codecs of its own. A batch
reaches across sublevels and is atomic: the record and its index entry are both written
or neither is, also when the process dies halfway.

```clojure
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
```

```clojure
(def book (stores c))

(place-order! c book {:id "0001" :customer "ada" :amount 120})
(place-order! c book {:id "0002" :customer "bob" :amount 75})
(place-order! c book {:id "0003" :customer "ada" :amount 310})
(place-order! c book {:id "0004" :customer "adam" :amount 9})

(orders-of! book "ada")
;; resolves to
[{:id "0001" :customer "ada" :amount 120}
 {:id "0003" :customer "ada" :amount 310}]

(db/keys! c)
;; resolves to the keys as they are stored
["!by-customer!ada/0001" "!by-customer!ada/0003" "!by-customer!adam/0004"
 "!by-customer!bob/0002" "!orders!0001" "!orders!0002" "!orders!0003"
 "!orders!0004"]
```

The range of `orders-of!` runs from `"ada/"` up to `"ada0"`, the character after the
slash, so the orders of `adam` are not among it.

### A range as a stream

An iterator channel reads the range in pieces and waits for its consumer: a full channel
buffer stops the reads until the consumer has caught up. Closing the channel ends the
scan and releases the iterator.

```clojure
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
```

```clojure
(total! book 100)
;; resolves to
514

(first-over! book 200)
;; resolves to
{:id "0003" :customer "ada" :amount 310}
```

An error while reading arrives as the last value of the channel, and `iter/reduce!` and
`iter/collect!` reject with it. In a loop of your own, test a value with `iter/error?`.

### Follow the writes

```clojure
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
```

```clojure
(def ch (follow! c prn))

(db/put! c "a" {:n 1})
(db/batch! c [{:type :put :key "b" :value {:n 2}}
              {:type :del :key "a"}])
(db/clear! c {:gte "b"})
;; prints
{:event :put :key "a" :value {:n 1}}
{:event :batch :ops [{:type :put :key "b" :value {:n 2}} {:type :del :key "a"}]}
{:event :clear :gte "b"}
```

A write of one operation is a `:put` or a `:del`, a write of several is a `:batch`.
Without `:only` the channel carries the events of the lifecycle as well: `:opening`,
`:open`, `:closing`, `:closed`. The events are those of this process: LevelDB admits one
process per directory.

## Sizes are yours

The library decides nothing about the tuning of LevelDB. The options of `open!` that
concern it (`:cache-size`, `:write-buffer-size`, `:block-size`, `:max-open-files`,
`:max-file-size`, `:compression`) are handed to `level` as given, and one that is absent
is absent for `level` as well, whose
own default then applies:

```clojure
(db/open! {:path "data"
           :value-encoding :edn
           :cache-size 67108864
           :write-buffer-size 16777216})

(db/put! c "k" {:n 1} {:sync true})
```

What the library sets itself concerns the channels and the keys, and all of it can be
overridden:

| Option | Where | Absent |
|--------|-------|--------|
| `:batch-size` | `iter/entries!`, `iter/keys!`, `iter/values!` | 100 entries are fetched per read |
| `:buffer` | the same, and `db/events!` | the channel buffers 32 values |
| `:key-encoding`, `:value-encoding` | `open!`, a sublevel, a single call | `:utf8` at `open!`; a sublevel takes those of its parent; a call takes those of its client |
| `:separator` | `db/sublevel` | `"!"` |
| `:limit` | every range | no limit |

A range without a `:limit` read through `db/all!`, `db/keys!` or `db/values!` is in
memory as a whole. For a range that may be large, give a `:limit` or read it as a
channel.

## Features

`c` is an open client with `:edn` values on disk, and `orders` is
`(db/sublevel c "orders")`.

### Codecs: `leveldb-cljs.codec`

A codec turns a key or a value into what is stored and back. `:utf8` takes strings,
`:json` and `:edn` take data, `:hex` and `:base64` take a string and store the bytes,
`:buffer`, `:view` and `:raw` take bytes. A codec of your own is a map of `:format`,
`:encode` and `:decode`. A value that the codec does not take is refused before anything
is stored.

```clojure
(codec/names)
;; => [:base64 :buffer :edn :hex :json :raw :utf8 :view]

(db/value-encoding c)
;; => {:name :edn, :format :utf8, :upstream "utf8"}

(db/key-encoding c)
;; => {:name :utf8, :format :utf8, :upstream "utf8"}
```

### Reads, writes, ranges, snapshots and batches: `leveldb-cljs.core`

```clojure
(p/let [_        (db/put! c "a" {:n 1})
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
        open     (db/open-resources c)]
  ...)
```

The bindings resolve to:

```clojure
{:both [{:n 1} {:n 2} nil]
 :there? true
 :as-text "{:n 1}"
 :seen ["a" "b"]
 :now ["b" "c" "d"]
 :tail [{:key "d" :value {:n 4}}]
 :left ["b"]
 :open {:iterators 0 :snapshots 0 :batches 0 :subscriptions 0}}
```

| Function | Result |
|----------|--------|
| `open!`, `close!`, `status`, `open?`, `supports` | the life of a client. `open!` resolves to a client that is open; `open!` of a closed client opens it again |
| `get!`, `get-many!`, `has!`, `has-many!` | reads; `nil` for a key that is absent |
| `get-sync` | a read that returns the value. It blocks the event loop while it reads |
| `put!`, `del!`, `batch!` | writes; a batch is atomic |
| `clear!` | deletes a range or everything. It is not atomic |
| `all!`, `keys!`, `values!` | a range as a vector: `:gt` `:gte` `:lt` `:lte` `:reverse` `:limit` |
| `snapshot`, `close-snapshot!`, `with-snapshot!` | reads of one state of the database, whatever is written meanwhile (`:seen` above) |
| `chained-batch`, `batch-put!`, `batch-del!`, `batch-write!`, `with-batch!` | a batch that is built up step by step, then written at once |
| `iterator`, `it-next!`, `it-nextv!`, `it-seek!`, `it-close!` | an iterator by hand, with `seek` |
| `events!` | the writes and the lifecycle as a channel |
| `open-resources` | the iterators, snapshots, batches and subscriptions that are still open |

```clojure
(db/status c)
;; => :open

(db/backend c)
;; => :level

(select-keys (db/supports c) [:explicit-snapshots :permanence :has :get-sync])
;; => {:explicit-snapshots true, :permanence true, :has true, :get-sync true}
```

### Sublevels

```clojure
(db/prefix orders)
;; => "!orders!"

(db/path (db/sublevel orders "open"))
;; => ["orders" "open"]

(db/prefix (db/sublevel c "orders" {:separator "/"}))
;; => "/orders/"

(db/value-encoding (db/sublevel c "blobs" {:value-encoding :hex}))
;; => {:name :hex, :format :buffer, :upstream "hex"}
```

A sublevel client outlives `close!` and `open!` of its database.

### Iterators as channels: `leveldb-cljs.iter`

`entries!`, `keys!` and `values!` return a channel over a range; `collect!` and `reduce!`
drain one into a promise; `entries-seq!`, `keys-seq!` and `values-seq!` do both.

### Bytes

Byte values are opaque. They are made, read and compared without interop:

```clojure
(db/bytes->hex (db/utf8->bytes "level"))
;; => "6c6576656c"

(db/bytes->utf8 (db/base64->bytes "bGV2ZWw="))
;; => "level"

(db/bytes= (db/hex->bytes "00ff") (db/base64->bytes "AP8="))
;; => true
```

### A database in memory

`(db/open! {:memory? true})` opens a database in the memory of the process, on the
`memory-level` package, a development dependency of the checkout that a project of your own
then installs too. It has the same API and leaves nothing behind, which suits tests.

## Error handling

Functions ending in `!` reject and never throw; the few that return a value (`get-sync`,
`sublevel`, `snapshot`, `iterator`, `chained-batch`, `it-seek!`, `batch-put!`,
`batch-del!`, `batch-clear!`) throw. Either way the error is an `ex-info` whose `ex-data`
carries a `:type`:

| `:type` | Meaning |
|---------|---------|
| `:leveldb-cljs/argument-error` | an argument was refused before the database was called (`:arg`, `:op`) |
| `:leveldb-cljs/codec-error` | a value could not be encoded or decoded (`:codec`, `:direction`, `:role`) |
| `:leveldb-cljs/db-error` | the database raised it: `:code` is the code of `level` in kebab-case, such as `:database-not-open`, `:iterator-not-open`, `:not-supported`; `:op` is the operation |

`db/error-data`, `db/error-type`, `db/error-code` and `db/not-open?` read an error. A
read with every failure turned into data, here on a client with the `:utf8` codec for its
values:

```clojure
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
```

```clojure
(read! c "a")
;; resolves to
{:value "1"}

(db/close! c)
(read! c "a")
;; resolves to
{:failed {:type :leveldb-cljs/db-error :code :database-not-open :op :get}}
```

A directory that another client holds open cannot be opened. `open!` waits until the
database is open, so the failure is its rejection and not that of the first read:

```clojure
(-> (db/open! {:path "data"})
    (p/catch (fn [e] (explain e))))
;; resolves to
{:type :leveldb-cljs/db-error :code :database-not-open :reason :locked :op :open}
```

`:reason` is `:locked`, `:missing` (with `:create-if-missing? false`), `:exists` (with
`:error-if-exists? true`) or `:io`.

A map under the `:utf8` codec, and an option that does not exist:

```clojure
(db/put! c "b" {:n 1})
;; rejects with the ex-data
{:type :leveldb-cljs/codec-error :codec :utf8 :direction :encode :role :value :value {:n 1}}

(db/get! c "a" {:encoding :utf8})
;; rejects with the ex-data
{:type :leveldb-cljs/argument-error :arg :opts :op :get :unknown #{:encoding}}
```

## Reference

- [`docs/leveldb-cljs.org`](docs/leveldb-cljs.org) is the manual: clients, codecs, the
  operations, batches, sublevels, iterators, the events channel, `clear!` and
  persistence, error handling, and an index of every public function by namespace.
- [`docs/leveldb-api-inventory.org`](docs/leveldb-api-inventory.org) maps the API of
  `level` to the functions of this library, with the error codes, the encodings and the
  fine points of iterators, batches and snapshots.

Not exposed: the maintenance functions of `classic-level` (`approximateSize`,
`compactRange`, `getProperty`, `destroy`, `repair`).

Every public function carries a docstring; the namespaces are `leveldb-cljs.core`,
`leveldb-cljs.iter`, `leveldb-cljs.codec` and `leveldb-cljs.util`.

## Development

```sh
./run-tests.sh
```

runs the documentation gate and the test suite. It uses no network: the databases are in
memory or in temp directories that are removed afterwards. The examples of this README
are part of it: the suite fails when the README shows code or values that differ from
what runs.

```sh
npx shadow-cljs compile examples && node target/examples.js
```

runs the quick start.

```sh
npx shadow-cljs compile smoke && node target/smoke.js
```

drives the library against a database on disk in a temp directory, section by section.

The library targets Node.js: `level` stores on disk through a native module there. There
is no browser build.

## License

MIT
