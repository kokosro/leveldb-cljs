(ns leveldb-cljs.iter-test
  (:require [cljs.core.async :as async :refer [<! go-loop]]
            [cljs.test :refer [deftest is async]]
            [promesa.core :as p]
            [leveldb-cljs.core :as core]
            [leveldb-cljs.iter :as iter]
            [leveldb-cljs.test-util :as tu]
            [leveldb-cljs.util :as util]))

(defn- drain
  "Promise of a vector of everything taken from `ch` until it closes
   (errors included as items)."
  [ch]
  (p/create
   (fn [resolve _]
     (go-loop [acc []]
       (if-some [x (<! ch)]
         (recur (conj acc x))
         (resolve acc))))))

(defn- take-n
  "Promise of the first n items of ch."
  [ch n]
  (p/create
   (fn [resolve _]
     (go-loop [acc [] n n]
       (if (pos? n)
         (if-some [x (<! ch)]
           (recur (conj acc x) (dec n))
           (resolve acc))
         (resolve acc))))))

(defn- seeded
  "Puts n numbered entries k00.. into c."
  [c n]
  (tu/seed! c (for [i (range n)] [(str "k" (if (< i 10) "0" "") i) (str "v" i)])))

(deftest entries-channel-drains-in-order-on-both-backends
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c backend]
          (p/let [_ (seeded c 5)
                  items (drain (iter/entries! c))
                  again (drain (iter/entries! c {:batch-size 2 :buffer 1}))
                  res (core/open-resources c)]
            (is (= [{:key "k00" :value "v0"} {:key "k01" :value "v1"} {:key "k02" :value "v2"}
                    {:key "k03" :value "v3"} {:key "k04" :value "v4"}]
                   items)
                (str backend))
            (is (= items again) "small batches and buffer yield the same sequence")
            (is (= 0 (:iterators res)) "the upstream iterators were closed at the end")))))))

(deftest keys-and-values-channels
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (seeded c 4)
                  ks (drain (iter/keys! c))
                  vs (drain (iter/values! c))
                  rks (drain (iter/keys! c {:reverse true :limit 2}))
                  res (core/open-resources c)]
            (is (= ["k00" "k01" "k02" "k03"] ks))
            (is (= ["v0" "v1" "v2" "v3"] vs))
            (is (= ["k03" "k02"] rks))
            (is (= 0 (:iterators res)))))))))

(deftest ranges-reverse-limit-and-only-flags
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (tu/seed! c {"" "empty" "a" "1" "b" "2" "c" "3"})
                  gte (drain (iter/keys! c {:gte "b"}))
                  gt (drain (iter/keys! c {:gt "b"}))
                  lte (drain (iter/keys! c {:lte "b"}))
                  lt (drain (iter/keys! c {:lt "b"}))
                  empty-lte (drain (iter/keys! c {:lte ""}))
                  empty-gt (drain (iter/keys! c {:gt ""}))
                  limit0 (drain (iter/keys! c {:limit 0}))
                  rev (drain (iter/entries! c {:reverse true :limit 2}))
                  ko (drain (iter/entries! c {:keys-only? true :gte "b"}))
                  vo (drain (iter/entries! c {:values-only? true :gte "b"}))
                  none (drain (iter/entries! c {:gt "zzz"}))
                  res (core/open-resources c)]
            (is (= ["b" "c"] gte))
            (is (= ["c"] gt))
            (is (= ["" "a" "b"] lte))
            (is (= ["" "a"] lt))
            (is (= [""] empty-lte))
            (is (= ["a" "b" "c"] empty-gt))
            (is (= [] limit0) "limit 0 closes an empty channel")
            (is (= [{:key "c" :value "3"} {:key "b" :value "2"}] rev))
            (is (= [{:key "b"} {:key "c"}] ko))
            (is (= [{:value "2"} {:value "3"}] vo))
            (is (= [] none) "an empty range closes immediately")
            (is (= 0 (:iterators res)))))))))

(deftest encoding-overrides-on-channels
  (async done
    (tu/finish done
      (tu/with-memory
        {:value-encoding :edn}
        (fn [c]
          (p/let [_ (core/put! c "a" {:n 1})
                  _ (core/put! c "b" {:n 2})
                  edn (drain (iter/values! c))
                  raw (drain (iter/values! c {:value-encoding :utf8}))
                  ks (drain (iter/keys! c {:key-encoding :raw}))
                  bad (drain (iter/values! c {:value-encoding :json}))]
            (is (= [{:n 1} {:n 2}] edn))
            (is (= ["{:n 1}" "{:n 2}"] raw))
            (is (= ["a" "b"] (mapv core/bytes->utf8 ks)) "keys as raw bytes")
            (is (= 1 (count bad)))
            (is (iter/error? (first bad)) "a decode failure arrives as the last item")
            (is (= :leveldb-cljs/codec-error (util/error-type (first bad))))))))))

(deftest closing-the-channel-cancels-the-iterator
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (seeded c 50)
                  ch (iter/entries! c {:batch-size 5 :buffer 2})
                  first-two (take-n ch 2)
                  _ (async/close! ch)
                  _ (tu/sleep (* 3 iter/cancel-poll-ms))
                  res (core/open-resources c)]
            (is (= [{:key "k00" :value "v0"} {:key "k01" :value "v1"}] first-two))
            (is (= 0 (:iterators res)) "the upstream iterator was released after close!")))))))

(deftest closing-a-never-consumed-channel-releases-the-iterator
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (seeded c 20)
                  ch (iter/entries! c {:batch-size 1 :buffer 1})
                  _ (tu/sleep 20)
                  parked (core/open-resources c)
                  _ (async/close! ch)
                  _ (tu/sleep (* 3 iter/cancel-poll-ms))
                  res (core/open-resources c)]
            (is (= 1 (:iterators parked)) "producer is parked on the full buffer")
            (is (= 0 (:iterators res)) "…and still notices the close")))))))

(deftest closed-database-yields-an-error-item
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (seeded c 3)
                  _ (core/close! c)
                  items (drain (iter/entries! c))
                  ks (drain (iter/keys! c))
                  e (tu/rejection (iter/entries-seq! c))
                  _ (core/open! c)]
            (is (= 1 (count items)))
            (is (iter/error? (first items)))
            (is (= :database-not-open (util/error-code (first items))))
            (is (util/not-open? (first ks)))
            (is (= :database-not-open (util/error-code e)) "collect! rejects with the error item")))))))

(deftest closing-the-database-mid-iteration-ends-the-channel-with-an-error
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (seeded c 30)
                  ch (iter/entries! c {:batch-size 2 :buffer 1})
                  head (take-n ch 2)
                  _ (core/close! c)
                  rest (drain ch)
                  _ (core/open! c)]
            (is (= 2 (count head)))
            (is (iter/error? (last rest)) "the last item is the not-open error")
            (is (util/not-open? (last rest)))
            (is (every? (complement iter/error?) (butlast rest)) "items before it are data")))))))

(deftest seq-helpers-and-collect
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c _]
          (p/let [_ (seeded c 4)
                  es (iter/entries-seq! c {:limit 2})
                  ks (iter/keys-seq! c {:gt "k01"})
                  vs (iter/values-seq! c {:reverse true})
                  col (iter/collect! (iter/keys! c {:lte "k00"}))
                  res (core/open-resources c)]
            (is (= [{:key "k00" :value "v0"} {:key "k01" :value "v1"}] es))
            (is (= ["k02" "k03"] ks))
            (is (= ["v3" "v2" "v1" "v0"] vs))
            (is (= ["k00"] col))
            (is (= 0 (:iterators res)))))))))

(deftest reduce-early-exit
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (seeded c 40)
                  n (iter/reduce! (fn [acc _] (if (= 3 acc) (reduced :stop) (inc acc))) 0
                                  (iter/entries! c {:batch-size 4 :buffer 2}))
                  all (iter/reduce! (fn [acc _] (inc acc)) 0 (iter/keys! c))
                  e (tu/rejection (iter/reduce! (fn [acc _] (inc acc)) 0 (iter/values! c {:value-encoding :json})))
                  _ (tu/sleep (* 3 iter/cancel-poll-ms))
                  res (core/open-resources c)]
            (is (= :stop n))
            (is (= 40 all))
            (is (= :leveldb-cljs/codec-error (util/error-type e)))
            (is (= 0 (:iterators res)) "early exit closed the channel and released the iterator")))))))

(deftest bad-channel-options-are-error-items
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (seeded c 2)
                  bs (drain (iter/entries! c {:batch-size 0}))
                  unk (drain (iter/keys! c {:bogus 1}))
                  lim (drain (iter/values! c {:limit -7}))
                  both (drain (iter/entries! c {:keys-only? true :values-only? true}))
                  bound (drain (iter/entries! c {:gte 5}))
                  res (core/open-resources c)]
            (doseq [[label items] {:batch-size bs :unknown unk :limit lim :both both}]
              (is (= 1 (count items)) (str label))
              (is (= :leveldb-cljs/argument-error (util/error-type (first items))) (str label)))
            (is (= :leveldb-cljs/codec-error (util/error-type (first bound))) "bounds go through the key codec")
            (is (= 0 (:iterators res)) "no iterator was created for invalid options")))))))

(deftest channels-on-sublevels-and-snapshots
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [sub (core/sublevel c "s")
                  _ (tu/seed! sub {"a" "1" "b" "2"})
                  _ (core/put! c "root" "r")
                  snap (core/snapshot c)
                  _ (core/put! sub "c" "3")
                  sub-keys (drain (iter/keys! sub))
                  snap-keys (drain (iter/keys! c {:snapshot snap}))
                  _ (core/close-snapshot! snap)
                  root-keys (drain (iter/keys! c))]
            (is (= ["a" "b" "c"] sub-keys))
            (is (= ["!s!a" "!s!b" "root"] snap-keys) "the snapshot predates the third put")
            (is (= ["!s!a" "!s!b" "!s!c" "root"] root-keys))))))))
