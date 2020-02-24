(ns io.mdrogalis.voluble.core
  (:require [io.mdrogalis.voluble.parse :as p]
            [io.mdrogalis.voluble.extract :as e]
            [io.mdrogalis.voluble.compile :as c]
            [io.mdrogalis.voluble.generate :as g])
  (:import [java.util Properties]
           [com.github.javafaker Faker]))


;; TODO Pre-parse configurations
;; TODO Push all generator work to compilation phase
;; TODO Write tests
;; TODO Knob to control generation frequency, both globally + per topic
;; TODO Validation on properties

(def max-attempts 100)

(def default-max-history 1000000)

(defn make-context [m]
  (let [kvs (p/parse-keys m)
        global-cfgs (e/extract-global-configs kvs)
        topic-cfgs (e/extract-topic-configs kvs)
        attr-cfgs (e/extract-attr-configs kvs)
        generators (e/extract-generators kvs)
        compiled (c/compile-generator-strategies generators)]
    (when (empty? kvs)
      (throw (ex-info "No usable properties - refusing to start since there is no work to do." {})))
    {:faker (Faker.)
     :global-configs global-cfgs
     :topic-configs topic-cfgs
     :attr-configs attr-cfgs
     :generators compiled
     :topic-seq (cycle (keys compiled))}))

(defn max-history-for-topic [context topic]
  (if-let [topic-max (get-in context [:topic-configs topic "history" "records" "max"])]
    (Integer/parseInt topic-max)
    (if-let [global-max (get-in context [:global-configs "history" "records" "max"])]
      (Integer/parseInt global-max)
      default-max-history)))

(defn purge-history [context topic]
  (let [history (get-in context [:history topic])
        max-history (max-history-for-topic context topic)
        n (count history)]
    (if (>= n max-history)
      (update-in context [:history topic] subvec 1 n)
      context)))

(defn advance-step [context]
  (let [topic (first (:topic-seq context))
        deps (get-in context [:generators topic :dependencies])
        key-gens (get-in context [:generators topic :key])
        val-gens (get-in context [:generators topic :value])
        key-results (g/invoke-key-generator context deps key-gens)
        val-results (g/invoke-value-generator context topic deps val-gens)
        event {:key (:result key-results) :value (:result val-results)}]
    (if (and (:success? key-results) (:success? val-results))
      (-> context
          (assoc-in [:generated :success?] true)
          (assoc-in [:generated :topic] topic)
          (assoc-in [:generated :event :key] (:result key-results))
          (assoc-in [:generated :event :value] (:result val-results))
          (update-in [:history topic] (fnil conj []) event)
          (update :topic-seq rest)
          (purge-history topic))
      (-> context
          (dissoc :generated)
          (assoc-in [:generated :success?] false)
          (update :topic-seq rest)))))

(defn advance-until-success [context]
  (loop [new-context (advance-step context)
         attempts 0]
    (if (> attempts max-attempts)
      (throw (ex-info "Couldn't generate another event. State machine may be deadlocked."
                      {:context context}))
      (if (get-in new-context [:generated :success?])
        new-context
        (recur (advance-step new-context) (inc attempts))))))
