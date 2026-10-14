(ns leveldb-cljs.dev
  "Node-script entry point for interactive development."
  (:require [leveldb-cljs.util :as util]))

(defn main [& _]
  (println "leveldb-cljs dev runtime loaded. Attach with: npx shadow-cljs cljs-repl dev")
  (println "error types:" (sort util/error-types))
  (js/setInterval #(println "…dev heartbeat") 60000))
