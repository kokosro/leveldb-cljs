(ns leveldb-cljs.events-test
  "The events! channel (inventory §6) on memory-level and on a
   temporary on-disk database. Network-free."
  (:require [cljs.core.async :as async :refer [<! go-loop]]
            [cljs.test :refer [deftest is async]]
            [promesa.core :as p]
            [leveldb-cljs.core :as core]
            [leveldb-cljs.test-util :as tu]
            [leveldb-cljs.util :as util]))

(defn- drain
  "Promise of a vector of everything taken from `ch` until it closes."
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

(deftest write-clear-and-lifecycle-events-arrive-in-order-on-both-backends
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c backend]
          (let [ch (core/events! c)]
            (p/let [_ (core/put! c "a" "1")
                    _ (core/del! c "a")
                    _ (core/batch! c [{:type :put :key "b" :value "2"} {:type :del :key "c"}])
                    _ (core/batch! c [{:type :put :key "d" :value "4"}])
                    _ (core/with-batch! c (fn [b] (core/batch-put! b "e" "5") (core/batch-del! b "f")))
                    _ (core/clear! c {:gte "x" :lt "y"})
                    _ (core/clear! c)
                    during (core/open-resources c)
                    _ (core/close! c)
                    evs (drain ch)
                    after (core/open-resources c)]
              (is (= [{:event :put :key "a" :value "1"}
                      {:event :del :key "a"}
                      {:event :batch :ops [{:type :put :key "b" :value "2"} {:type :del :key "c"}]}
                      {:event :put :key "d" :value "4"}
                      {:event :batch :ops [{:type :put :key "e" :value "5"} {:type :del :key "f"}]}
                      {:event :clear :gte "x" :lt "y"}
                      {:event :clear}
                      {:event :closing}
                      {:event :closed}]
                     evs)
                  (str backend))
              (is (= 1 (:subscriptions during)) "the subscription is tracked while live")
              (is (= 0 (:subscriptions after)) "the :closed event releases the listeners"))))))))

(deftest reopen-emits-opening-and-open-and-a-new-subscription-sees-them
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [_ (core/close! c)
                  ch (core/events! c)
                  _ (core/open! c)
                  _ (core/put! c "k" "v")
                  evs (take-n ch 3)]
            (is (= [{:event :opening} {:event :open} {:event :put :key "k" :value "v"}] evs))
            (async/close! ch)))))))

(deftest sublevel-events-on-the-sublevel-and-on-the-root
  (async done
    (tu/finish done
      (tu/with-each-backend
        (fn [c backend]
          (let [people (core/sublevel c "people")
                root-ch (core/events! c {:only #{:put :del :batch :clear}})
                sub-ch (core/events! people {:only #{:put :del :batch :clear}})]
            (p/let [_ (core/put! people "alice" "x")
                    _ (core/batch! c [{:type :put :key "bob" :value "y" :sublevel people}])
                    _ (core/del! people "alice")
                    _ (core/clear! people {:gte "b"})
                    root-evs (take-n root-ch 4)
                    sub-evs (take-n sub-ch 3)]
              (is (= [{:event :put :key "!people!alice" :value "x"}
                      {:event :put :key "!people!bob" :value "y" :sublevel true}
                      {:event :del :key "!people!alice"}]
                     (take 3 root-evs))
                  (str backend ": the root sees prefixed keys, :sublevel only for routed ops"))
              (is (= :clear (:event (nth root-evs 3))) (str backend))
              (is (= "!people!b" (:gte (nth root-evs 3))) (str backend ": the root sees the prefixed bound"))
              (is (= [{:event :put :key "alice" :value "x"}
                      {:event :del :key "alice"}
                      {:event :clear :gte "b"}]
                     sub-evs)
                  (str backend ": the sublevel sees its own keys; a root batch routed to it is not its event"))
              (async/close! root-ch)
              (async/close! sub-ch))))))))

(deftest values-are-decoded-with-the-client-codecs-and-decode-failures-are-reported
  (async done
    (tu/finish done
      (tu/with-memory {:value-encoding :edn}
        (fn [c]
          (let [ch (core/events! c {:only #{:put}})
                plain (core/sublevel c "plain" {:value-encoding :utf8})]
            (p/let [_ (core/put! c "k" {:a [1 2] :b #{:x}})
                    _ (core/put! c "s" "text" {:value-encoding :utf8})
                    _ (core/put! plain "p" "}{ not edn")
                    [e1 e2 e3] (take-n ch 3)]
              (is (= {:event :put :key "k" :value {:a [1 2] :b #{:x}}} e1) "EDN data round-trips")
              (is (= {:event :put :key "s" :value 'text} e2)
                  "a per-call override is invisible: the stored text is read by the client codec")
              (is (= "!plain!p" (:key e3)))
              (is (= "}{ not edn" (:value e3)) "the raw text is kept")
              (is (= :leveldb-cljs/codec-error (util/error-type (:decode-error e3))))
              (async/close! ch))))))))

(deftest closing-the-channel-releases-the-listeners
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (let [idle (core/events! c)
                parked (core/events! c {:buffer 1 :only #{:put}})]
            (p/let [_ (core/put! c "a" "1")
                    _ (core/put! c "b" "2")
                    _ (core/put! c "c" "3")
                    [first-idle] (take-n idle 1)
                    before (core/open-resources c)
                    _ (do (async/close! idle) (async/close! parked))
                    _ (tu/sleep (* 3 util/cancel-poll-ms))
                    after (core/open-resources c)
                    _ (core/put! c "d" "4")]
              (is (= {:event :put :key "a" :value "1"} first-idle))
              (is (= 2 (:subscriptions before)))
              (is (= 0 (:subscriptions after))
                  "both the idle loop and the parked put noticed the close within the poll interval"))))))))

(deftest events-option-errors-arrive-as-the-only-item
  (async done
    (tu/finish done
      (tu/with-memory
        (fn [c]
          (p/let [bad-opt (drain (core/events! c {:bogus 1}))
                  bad-only (drain (core/events! c {:only #{:put :nope}}))
                  bad-buffer (drain (core/events! c {:buffer 0}))
                  not-client (drain (core/events! "nope"))
                  res (core/open-resources c)]
            (doseq [[label evs] {"unknown option" bad-opt "bad :only" bad-only
                                 "bad :buffer" bad-buffer "not a client" not-client}]
              (is (= 1 (count evs)) label)
              (is (= :leveldb-cljs/argument-error (util/error-type (first evs))) label))
            (is (= 0 (:subscriptions res)) "no listener was registered")))))))
