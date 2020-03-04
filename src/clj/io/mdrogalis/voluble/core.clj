(ns io.mdrogalis.voluble.core
  (:require [io.mdrogalis.voluble.parse :as p]
            [io.mdrogalis.voluble.extract :as e]
            [io.mdrogalis.voluble.compile :as c]
            [io.mdrogalis.voluble.generate :as g])
  (:import [java.util Properties]
           [com.github.javafaker Faker]))

;; TODO Write tests
;; TODO Knob to control generation frequency, both globally + per topic // throttling


;; TODO bounded data sets
;; TODO timestamp math
;; TODO distributions
;; TODO Scoped events

(def max-attempts 100)

(def default-max-history 1000000)

(def default-generator (constantly {:success? true}))

(defn add-global-configs [context kvs]
  (merge context {:global-configs (e/extract-global-configs kvs)}))

(defn add-topic-configs [context kvs]
  (merge context {:topic-configs (e/extract-topic-configs kvs)}))

(defn add-attr-configs [context kvs]
  (merge context {:attr-configs (e/extract-attr-configs kvs)}))

(defn add-generators [context kvs]
  (merge context {:generators (e/extract-generators kvs)}))

(defn add-topic-sequencing [context]
  (merge context {:topic-seq (cycle (keys (:generators context)))}))

(defn make-context [m]
  (when (empty? m)
    (throw (ex-info "No usable properties - refusing to start since there is no work to do." {})))

  (let [kvs (p/parse-keys m)]
    (-> {:faker (Faker.)}
        (add-global-configs kvs)
        (add-topic-configs kvs)
        (add-attr-configs kvs)
        (add-generators kvs)
        (c/compile-generator-strategies)
        (add-topic-sequencing))))

(defn max-history-for-topic [context topic]
  (if-let [topic-max (get-in context [:topic-configs topic "history" "records" "max"])]
    topic-max
    (if-let [global-max (get-in context [:global-configs "history" "records" "max"])]
      global-max
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
        key-gen-f (get-in context [:generators topic :key] default-generator)
        val-gen-f (get-in context [:generators topic :value] default-generator)
        {:keys [key-results val-results]} (g/invoke-generator context deps key-gen-f val-gen-f)]
    (if (and (:success? key-results) (:success? val-results))
      (let [event {:key (:result key-results) :value (:result val-results)}]
        (-> context
            (assoc-in [:generated :success?] true)
            (assoc-in [:generated :topic] topic)
            (assoc-in [:generated :event :key] (:result key-results))
            (assoc-in [:generated :event :value] (:result val-results))
            (update-in [:history topic] (fnil conj []) event)
            (update :topic-seq rest)
            (purge-history topic)))
      (-> context
          (dissoc :generated)
          (assoc-in [:generated :success?] false)
          (update :topic-seq rest)))))

(defn advance-until-success [context]
  (loop [new-context (advance-step context)
         attempts 0]
    (if (> attempts max-attempts)
      (throw (ex-info "Couldn't generate another event. State machine may be livelocked."
                      {:context context}))
      (if (get-in new-context [:generated :success?])
        new-context
        (recur (advance-step new-context) (inc attempts))))))
