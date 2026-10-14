(ns leveldb-cljs.iter
  "Iterators as core.async channels. Pure
   ClojureScript: uses only leveldb-cljs.core handles and core.async.

   (entries! c opts) / (keys! c opts) / (values! c opts) return a channel
   that yields, respectively, {:key k :value v} maps, keys, or values in
   key order, then closes. Reads are lazy and backpressured: the producer
   fetches :batch-size items per upstream read (default 100) and parks
   when the channel buffer (:buffer, default 32) is full, so at most one
   upstream read is ever in flight per iterator.

   Errors: an error (a leveldb-cljs ex-info: db-error / codec-error /
   argument-error) is put on the channel as its LAST item, then the channel
   closes; test items with `error?`. collect! / entries-seq! turn that into
   a rejected promise.

   Cancellation: closing the channel (core.async/close!) cancels the
   iteration: the producer notices at its next put (immediately when it is
   parked on a full buffer, otherwise within `cancel-poll-ms`) and closes
   the upstream iterator, so nothing leaks (verify with
   core/open-resources). Abort signals are not exposed.

   seek is deliberately NOT available on a channel: the upstream iterator
   is busy whenever a read is in flight and `seek` then throws
   :iterator-busy; use core/iterator + core/it-seek! for
   manual positioning."
  (:require [cljs.core.async :as async :refer [<! go-loop]]
            [promesa.core :as p]
            [leveldb-cljs.core :as core]
            [leveldb-cljs.util :as util]))

(def default-batch-size
  "Items fetched per upstream read when :batch-size is not given."
  100)

(def default-buffer
  "Channel buffer size when :buffer is not given."
  32)

(def cancel-poll-ms
  "How long a parked put waits before re-checking whether the consumer
   closed the channel (a closed channel does not wake parked puts). Shared
   with the events channel of leveldb-cljs.core (util/cancel-poll-ms)."
  util/cancel-poll-ms)

(def channel-options
  "Options of the channel producers besides the core iterator options."
  #{:batch-size :buffer})

(defn error?
  "True when a channel item is an error (a leveldb-cljs ex-info) rather
   than data."
  [x]
  (some? (util/error-data x)))

(defn- promise->ch
  "Bridges a promise to a promise-chan delivering [:ok v] or [:error e]."
  [prom]
  (let [ch (async/promise-chan)]
    (-> (p/promise prom)
        (p/then (fn [v] (async/put! ch [:ok v])))
        (p/catch (fn [e] (async/put! ch [:error e]))))
    ch))

(def ^:private put-or-cancel! util/put-or-cancel!)

(defn- put-all!
  "Puts every item of `items`; yields false as soon as the consumer closed."
  [ch items]
  (go-loop [items (seq items)]
    (if items
      (if (<! (put-or-cancel! ch (first items)))
        (recur (next items))
        false)
      true)))

(defn- produce!
  "Drives iterator handle `it` into `ch` until the end, an error, or the
   consumer closing `ch`. The upstream iterator is closed in every case,
   and BEFORE the channel closes, so a consumer that sees the channel end
   knows the iterator is released."
  [it ch batch-size]
  (go-loop []
    (let [[tag x] (<! (promise->ch (core/it-nextv! it batch-size)))]
      (case tag
        :error (do (<! (promise->ch (core/it-close! it)))
                   (<! (put-or-cancel! ch x))
                   (async/close! ch))
        :ok (cond
              (empty? x) (do (<! (promise->ch (core/it-close! it)))
                             (async/close! ch))
              (<! (put-all! ch x)) (recur)
              :else (<! (promise->ch (core/it-close! it))))))))

(defn- channel-of
  [op ctor c opts]
  (let [opts (util/normalize-opts opts)
        batch-size (get opts :batch-size default-batch-size)
        buffer (get opts :buffer default-buffer)
        ch (async/chan buffer)]
    (try
      (when-not (and (integer? batch-size) (pos? batch-size))
        (throw (util/argument-error (str (name op) ": :batch-size must be a positive integer")
                                    {:arg :batch-size :value batch-size :op op})))
      (let [it (ctor c (apply dissoc opts channel-options))]
        (produce! it ch batch-size))
      (catch :default e
        (async/put! ch e)
        (async/close! ch)))
    ch))

(defn entries!
  "Channel of {:key k :value v} maps in key order, closed after the last
   one. opts: the core/iterator options — :gt :gte :lt :lte :reverse :limit
   :keys-only? :values-only? (the other slot is omitted from the map)
   :key-encoding :value-encoding :snapshot :fill-cache
   :high-water-mark-bytes — plus :batch-size and :buffer. Close the channel
   to stop early; an error arrives as the last item (see `error?`)."
  [c & [opts]]
  (channel-of :entries core/iterator c opts))

(defn keys!
  "Channel of keys in key order (core/key-iterator opts + :batch-size, :buffer)."
  [c & [opts]]
  (channel-of :keys core/key-iterator c opts))

(defn values!
  "Channel of values in key order (core/value-iterator opts + :batch-size, :buffer)."
  [c & [opts]]
  (channel-of :values core/value-iterator c opts))

(defn collect!
  "Drains a channel into a promise of a vector. An error item rejects the
   promise with that error (the channel is closed by the producer right
   after it)."
  [ch]
  (p/create
   (fn [resolve reject]
     (go-loop [acc (transient [])]
       (let [x (<! ch)]
         (cond
           (nil? x) (resolve (persistent! acc))
           (error? x) (reject x)
           :else (recur (conj! acc x))))))))

(defn reduce!
  "Reduces a channel with (f acc item) into a promise of the final
   accumulator; an error item rejects. Use (reduced v) to stop early (the
   channel is then closed, cancelling the iterator)."
  [f init ch]
  (p/create
   (fn [resolve reject]
     (go-loop [acc init]
       (let [x (<! ch)]
         (cond
           (nil? x) (resolve acc)
           (error? x) (reject x)
           :else (let [acc (f acc x)]
                   (if (reduced? acc)
                     (do (async/close! ch) (resolve @acc))
                     (recur acc)))))))))

(defn entries-seq!
  "Convenience: promise of a vector of {:key k :value v} maps for the range
   (drains an entries! channel). Pass :limit for large ranges."
  [c & [opts]]
  (collect! (entries! c opts)))

(defn keys-seq!
  "Promise of a vector of keys (drains a keys! channel)."
  [c & [opts]]
  (collect! (keys! c opts)))

(defn values-seq!
  "Promise of a vector of values (drains a values! channel)."
  [c & [opts]]
  (collect! (values! c opts)))
