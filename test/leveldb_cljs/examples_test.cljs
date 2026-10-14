(ns leveldb-cljs.examples-test
  "The examples of the README, run. Network-free: the quick start works on
   a database in a fresh temp directory that is removed afterwards, the
   other examples on that and on the in-memory backend.

   The README is held to the code:
   - what stands between a README:begin and a README:end mark of
     dev/leveldb_cljs/examples.cljs stands in README.md as written;
   - every line the quick start prints stands in README.md;
   - every one-liner of `shown` stands in README.md with the value it
     evaluates to;
   - the results README.md shows for the other examples are the results
     they resolve to here.
   Spacing and commas are not compared."
  (:require ["fs" :as fs]
            [cljs.core.async :as async]
            [cljs.test :refer [deftest is testing async]]
            [clojure.string :as str]
            [leveldb-cljs.codec :as codec]
            [leveldb-cljs.core :as db]
            [leveldb-cljs.examples :as examples]
            [leveldb-cljs.iter :as iter]
            [leveldb-cljs.test-util :as tu]
            [promesa.core :as p])
  (:require-macros [leveldb-cljs.doctest :refer [claims]]))

;; ---------------------------------------------------------------------------
;; The one-liners
;; ---------------------------------------------------------------------------

(defn shown
  "The one-liners of the README, each with its value. `c` is an open client
   with :edn values."
  [c]
  (let [orders (db/sublevel c "orders")]
    (claims
     (codec/names)
     (db/value-encoding c)
     (db/key-encoding c)
     (db/status c)
     (db/backend c)
     (db/prefix orders)
     (db/path (db/sublevel orders "open"))
     (db/prefix (db/sublevel c "orders" {:separator "/"}))
     (db/value-encoding (db/sublevel c "blobs" {:value-encoding :hex}))
     (db/bytes->hex (db/utf8->bytes "level"))
     (db/bytes->utf8 (db/base64->bytes "bGV2ZWw="))
     (db/bytes= (db/hex->bytes "00ff") (db/base64->bytes "AP8="))
     (select-keys (db/supports c) [:explicit-snapshots :permanence :has :get-sync]))))

;; ---------------------------------------------------------------------------
;; What the README states the examples resolve to
;; ---------------------------------------------------------------------------

(def orders
  "The orders the examples place."
  [{:id "0001" :customer "ada" :amount 120}
   {:id "0002" :customer "bob" :amount 75}
   {:id "0003" :customer "ada" :amount 310}
   {:id "0004" :customer "adam" :amount 9}])

(def orders-shown
  "[{:id \"0001\" :customer \"ada\" :amount 120}
    {:id \"0003\" :customer \"ada\" :amount 310}]")

(def index-shown
  "[\"!by-customer!ada/0001\" \"!by-customer!ada/0003\" \"!by-customer!adam/0004\"
    \"!by-customer!bob/0002\" \"!orders!0001\" \"!orders!0002\" \"!orders!0003\"
    \"!orders!0004\"]")

(def total-shown
  "(total! book 100)
   ;; resolves to
   514")

(def first-over-shown
  "{:id \"0003\" :customer \"ada\" :amount 310}")

(def events-shown
  "{:event :put :key \"a\" :value {:n 1}}
   {:event :batch :ops [{:type :put :key \"b\" :value {:n 2}} {:type :del :key \"a\"}]}
   {:event :clear :gte \"b\"}")

(def tour-shown
  "{:both [{:n 1} {:n 2} nil]
    :there? true
    :as-text \"{:n 1}\"
    :seen [\"a\" \"b\"]
    :now [\"b\" \"c\" \"d\"]
    :tail [{:key \"d\" :value {:n 4}}]
    :left [\"b\"]
    :open {:iterators 0 :snapshots 0 :batches 0 :subscriptions 0}}")

(def found-shown
  "(read! c \"a\")
   ;; resolves to
   {:value \"1\"}")

(def closed-shown
  "{:failed {:type :leveldb-cljs/db-error :code :database-not-open :op :get}}")

(def locked-shown
  "{:type :leveldb-cljs/db-error :code :database-not-open :reason :locked :op :open}")

(def codec-shown
  "{:type :leveldb-cljs/codec-error :codec :utf8 :direction :encode :role :value :value {:n 1}}")

(def argument-shown
  "{:type :leveldb-cljs/argument-error :arg :opts :op :get :unknown #{:encoding}}")

;; ---------------------------------------------------------------------------
;; Reading the README
;; ---------------------------------------------------------------------------

(defn- slurp [path]
  (fs/readFileSync path "utf8"))

(defn- plain
  "`text` without what is not compared: commas, and any run of whitespace
   as one space."
  [text]
  (-> text
      (str/replace "," "")
      (str/replace #"\s+" " ")
      str/trim))

(defn- region
  "The text between the marks README:begin `label` and README:end `label`
   of the file at `path`, nil when either mark is missing."
  [path label]
  (let [[_ found] (re-find (re-pattern (str "(?s);; README:begin " label "\\n(.*?)\\n\\s*;; README:end " label))
                           (slurp path))]
    found))

(defn- printed
  "`x` as the README prints it."
  [x]
  (plain (pr-str x)))

(defn- ordered
  "`m` with the keys `ks` in that order (the order the README prints)."
  [m ks]
  (apply array-map (mapcat (fn [k] [k (get m k)]) (filter #(contains? m %) ks))))

(defn- drain
  "Promise of a vector of everything taken from `ch` until it closes."
  [ch]
  (p/create
   (fn [resolve _]
     (async/go-loop [acc []]
       (if-some [x (async/<! ch)]
         (recur (conj acc x))
         (resolve acc))))))

(defn- place-all! [c stores]
  (p/all (map #(examples/place-order! c stores %) orders)))

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest the-quick-start-reads-what-it-says
  (async done
    (tu/finish done
      (tu/with-tmp-dir
        (fn [dir]
          (p/let [{:keys [alice nobody present all-keys users last-one reopened]
                   :as read} (examples/quick-start dir)]
            (testing "every printed key is read, and no other"
              (is (= (set examples/printed) (set (keys read)))))
            (testing "a value comes back as the data that was put"
              (is (= {:name "Alice" :score 620} alice)))
            (testing "an absent key is nil, not an error"
              (is (nil? nobody)))
            (testing "the batch put three keys and deleted one"
              (is (= [false true] present))
              (is (= ["user:bob" "user:carol" "visit:1"] all-keys)))
            (testing "a range is bounded by keys, in key order"
              (is (= ["user:bob" "user:carol"] (mapv :key users)))
              (is (= [{:name "Bob" :score 10} {:name "Carol" :score 75}] (mapv :value users))))
            (testing "reverse with a limit is the last entry"
              (is (= [{:key "visit:1" :value {:user "bob"}}] last-one)))
            (testing "the data is on disk: it is there after close and open"
              (is (= {:name "Carol" :score 75} reopened))
              (is (some #(str/ends-with? % ".log") (tu/dir-files dir))))
            (testing "the database is closed: the directory can be opened again"
              (p/let [c (db/open! dir)
                      v (db/get! c "user:bob")
                      _ (db/close! c)]
                (is (= "{:name \"Bob\", :score 10}" v) "utf8 reads the stored EDN text")))))))))

(deftest main-cleans-up-after-itself
  (let [before (set (filter #(str/starts-with? % "quickstart-") (tu/dir-files (.tmpdir (js/require "os")))))
        dir (examples/temp-dir!)]
    (is (fs/existsSync dir))
    (is (not (contains? before (last (str/split dir #"/")))))
    (tu/rm-dir! dir)
    (is (not (fs/existsSync dir)))))

(deftest records-and-their-index
  (async done
    (tu/finish done
      (tu/with-each-backend {:value-encoding :edn}
        (fn [c backend]
          (let [stores (examples/stores c)]
            (p/let [_ (place-all! c stores)
                    of-ada (examples/orders-of! stores "ada")
                    of-adam (examples/orders-of! stores "adam")
                    of-nobody (examples/orders-of! stores "zoe")
                    root-keys (db/keys! c)
                    own-keys (db/keys! (:orders stores))]
              (testing (str backend ": the orders of a customer, and of no other")
                (is (= [(nth orders 0) (nth orders 2)] of-ada))
                (is (= (plain orders-shown)
                       (printed (mapv #(ordered % [:id :customer :amount]) of-ada))))
                (is (= [(nth orders 3)] of-adam) "ada is a prefix of adam")
                (is (= [] of-nobody)))
              (testing "a sublevel is a key prefix in one database"
                (is (= (plain index-shown) (printed root-keys)))
                (is (= ["0001" "0002" "0003" "0004"] own-keys)))
              (testing "a batch that cannot be written writes nothing"
                (p/let [e (tu/rejection
                           (db/batch! c [{:type :put :sublevel (:orders stores) :key "0005"
                                          :value {:id "0005" :customer "eve" :amount 1}}
                                         {:type :put :sublevel (:by-customer stores) :key "eve/0005"
                                          :value {:not "a string"}}]))
                        after (db/keys! (:orders stores))]
                  (is (= :leveldb-cljs/codec-error (db/error-type e)))
                  (is (= own-keys after)))))))))))

(deftest a-range-as-a-stream
  (async done
    (tu/finish done
      (tu/with-each-backend {:value-encoding :edn}
        (fn [c backend]
          (let [stores (examples/stores c)]
            (p/let [_ (place-all! c stores)
                    total-1 (examples/total! stores 1)
                    total-100 (examples/total! stores 100)
                    over (examples/first-over! stores 200)
                    none (examples/first-over! stores 1000)
                    _ (tu/sleep 250)
                    open (db/open-resources c)]
              (testing (str backend ": the sum does not depend on the size of a read")
                (is (= 514 total-1 total-100))
                (is (str/ends-with? (plain total-shown) (str " " total-100))))
              (testing "the first match ends the scan"
                (is (= (nth orders 2) over))
                (is (= (plain first-over-shown) (printed (ordered over [:id :customer :amount]))))
                (is (nil? none)))
              (testing "every iterator was released, the abandoned one as well"
                (is (= 0 (:iterators open)))))))))))

(deftest following-the-writes
  (async done
    (tu/finish done
      (tu/with-tmp-dir
        (fn [dir]
          (p/let [c (db/open! {:path dir :value-encoding :edn})]
            (let [seen (atom [])
                  ch (examples/follow! c #(swap! seen conj %))]
              (p/let [_ (db/put! c "a" {:n 1})
                      _ (db/batch! c [{:type :put :key "b" :value {:n 2}}
                                      {:type :del :key "a"}])
                      _ (db/clear! c {:gte "b"})
                      _ (tu/sleep 50)
                      during (db/open-resources c)
                      _ (db/close! c)
                      _ (tu/sleep 250)]
                (is (= [{:event :put :key "a" :value {:n 1}}
                        {:event :batch :ops [{:type :put :key "b" :value {:n 2}}
                                             {:type :del :key "a"}]}
                        {:event :clear :gte "b"}]
                       @seen))
                (is (= (plain events-shown)
                       (str/join " " (map #(printed (ordered % [:event :key :value :ops :gte])) @seen))))
                (is (= 1 (:subscriptions during)))
                (is (= 0 (:subscriptions (db/open-resources c))))
                (testing "the channel closed with the database"
                  (p/let [rest (drain ch)]
                    (is (= [] rest))))))))))))

(deftest the-features-once-each
  (async done
    (tu/finish done
      (tu/with-tmp-db {:value-encoding :edn}
        (fn [c]
          (p/let [tour (examples/tour! c)]
            (is (= {:both [{:n 1} {:n 2} nil]
                    :there? true
                    :as-text "{:n 1}"
                    :seen ["a" "b"]
                    :now ["b" "c" "d"]
                    :tail [{:key "d" :value {:n 4}}]
                    :left ["b"]
                    :open {:iterators 0 :snapshots 0 :batches 0 :subscriptions 0}}
                   tour))
            (is (= (plain tour-shown)
                   (printed (-> (ordered tour [:both :there? :as-text :seen :now :tail :left :open])
                                (update :open ordered [:iterators :snapshots :batches :subscriptions])))))))))))

(deftest a-failure-is-data
  (async done
    (tu/finish done
      (tu/with-tmp-dir
        (fn [dir]
          (p/let [c (db/open! {:path dir})
                  _ (db/put! c "a" "1")
                  found (examples/read! c "a")
                  absent (examples/read! c "zz")
                  locked (tu/rejection (db/open! {:path dir}))
                  codec (tu/rejection (db/put! c "b" {:n 1}))
                  argument (tu/rejection (db/get! c "a" {:encoding :utf8}))
                  _ (db/close! c)
                  closed (examples/read! c "a")]
            (testing "a read resolves, an absent key as well"
              (is (= {:value "1"} found))
              (is (str/ends-with? (plain found-shown) (printed found)))
              (is (= {:value nil} absent)))
            (testing "a read of a closed database"
              (is (= {:failed {:type :leveldb-cljs/db-error :code :database-not-open :op :get}}
                     closed))
              (is (= (plain closed-shown) (printed (update closed :failed ordered [:type :code :op])))))
            (testing "a directory that is open already"
              (is (= (plain locked-shown)
                     (printed (ordered (examples/explain locked) [:type :code :reason :op]))))
              (is (db/not-open? locked)))
            (testing "a value the codec does not take is refused before it is stored"
              (is (= (plain codec-shown)
                     (printed (ordered (db/error-data codec) [:type :codec :direction :role :value])))))
            (testing "an option that does not exist"
              (is (= :leveldb-cljs/argument-error (db/error-type argument)))
              (is (= (plain argument-shown)
                     (printed (ordered (db/error-data argument) [:type :arg :op :unknown])))))))))))

(deftest the-one-liners-hold
  (async done
    (tu/finish done
      (tu/with-tmp-db {:value-encoding :edn}
        (fn [c]
          (let [claimed (shown c)
                value (into {} claimed)]
            (is (= (count claimed) (count value)) "no one-liner is shown twice")
            (is (= [:base64 :buffer :edn :hex :json :raw :utf8 :view] (value "(codec/names)")))
            (is (= :edn (:name (value "(db/value-encoding c)"))))
            (is (= :utf8 (:name (value "(db/key-encoding c)"))))
            (is (= :open (value "(db/status c)")))
            (is (= :level (value "(db/backend c)")))
            (is (= "!orders!" (value "(db/prefix orders)")))
            (is (= ["orders" "open"] (value "(db/path (db/sublevel orders \"open\"))")))
            (is (= "6c6576656c" (value "(db/bytes->hex (db/utf8->bytes \"level\"))")))
            (is (= "level" (value "(db/bytes->utf8 (db/base64->bytes \"bGV2ZWw=\"))")))
            (is (every? (comp some? second) claimed))))))))

(deftest the-readme-shows-what-runs
  (async done
    (tu/finish done
      (tu/with-tmp-db {:value-encoding :edn}
        (fn [c]
          (let [readme (plain (slurp "README.md"))
                shows? (fn [text] (str/includes? readme (plain text)))]
            (testing "the marked code of the examples"
              (doseq [label ["quick-start" "index" "scan" "follow" "tour" "errors"]
                      :let [code (region "dev/leveldb_cljs/examples.cljs" label)]]
                (is (some? code) (str "no region " label))
                (is (shows? code) (str "README.md does not show " label ":\n" code))))
            (testing "every one-liner, with its value"
              (doseq [[source value] (shown c)
                      :let [line (str source "\n;; => " (pr-str value))]]
                (is (shows? line) (str "README.md does not show:\n" line))))
            (testing "what the examples resolve to"
              (doseq [text [orders-shown index-shown total-shown first-over-shown
                            events-shown found-shown
                            tour-shown closed-shown locked-shown codec-shown argument-shown]]
                (is (shows? text) (str "README.md does not show:\n" text))))
            (testing "the orders it places"
              (doseq [order orders]
                (is (shows? (printed (ordered order [:id :customer :amount]))))))
            (testing "the pointers it gives"
              (is (shows? "docs/leveldb-cljs.org"))
              (is (shows? "docs/leveldb-api-inventory.org"))
              (is (fs/existsSync "docs/leveldb-cljs.org"))
              (is (fs/existsSync "docs/leveldb-api-inventory.org")))))))))

(deftest the-readme-shows-what-the-quick-start-prints
  (async done
    (tu/finish done
      (tu/with-tmp-dir
        (fn [dir]
          (p/let [read (examples/quick-start dir)]
            (let [readme (plain (slurp "README.md"))]
              (doseq [k examples/printed
                      :let [line (str k " " (pr-str (get read k)))]]
                (is (str/includes? readme (plain line))
                    (str "README.md does not show the line:\n" line))))))))))
