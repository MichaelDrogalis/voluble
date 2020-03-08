(ns io.mdrogalis.voluble.core
  (:require [io.mdrogalis.voluble.parse :as p]
            [io.mdrogalis.voluble.extract :as e]
            [io.mdrogalis.voluble.compile :as c]
            [io.mdrogalis.voluble.generate :as g])
  (:import [java.util Properties]
           [com.github.javafaker Faker]))

(def max-failed-attempts 100)

(def default-max-history 1000000)

(def default-throttle-ms 0)

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

(defn initialize-topic-timestamps [context]
  (let [ts (zipmap (keys (:generators context)) (repeat Long/MIN_VALUE))]
    (assoc context :timestamps ts)))

(defn add-throttle-durations [context]
  (reduce
   (fn [ctx topic]
     (let [throttle-ms (or (get-in context [:topic-configs topic "throttle" "ms"])
                           (get-in context [:global-configs "throttle" "ms"])
                           default-throttle-ms)
           throttle-ns (* throttle-ms 1000000)]
       (assoc-in ctx [:generators topic :throttle-ns] throttle-ns)))
   context
   (keys (:generators context))))

(defn add-event-counters [context]
  (let [counters (zipmap (keys (:generators context)) (repeat 0))]
    (assoc context :records-produced counters)))

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
        (add-topic-sequencing)
        (initialize-topic-timestamps)
        (add-throttle-durations)
        (add-event-counters)
        (c/compile-retire-strategy))))

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
        last-ts (get-in context [:timestamps topic])
        throttle (get-in context [:generators topic :throttle-ns])
        next-ts (+ last-ts throttle)
        now (System/nanoTime)]
    (if (>= now next-ts)
      (let [deps (get-in context [:generators topic :dependencies])
            key-gen-f (get-in context [:generators topic :key] default-generator)
            val-gen-f (get-in context [:generators topic :value] default-generator)
            {:keys [key-results val-results]} (g/invoke-generator context deps key-gen-f val-gen-f)]
        (if (and (:success? key-results) (:success? val-results))
          (let [event {:key (:result key-results) :value (:result val-results)}
                retire-fn (get-in context [:generators topic :retire-fn])]
            (-> context
                (assoc-in [:generated :status] :success)
                (assoc-in [:generated :topic] topic)
                (assoc-in [:generated :event :key] (:result key-results))
                (assoc-in [:generated :event :value] (:result val-results))
                (assoc-in [:timestamps topic] now)
                (update-in [:history topic] (fnil conj []) event)
                (update-in [:records-produced topic] inc)
                (retire-fn)
                (purge-history topic)))
          (-> context
              (dissoc :generated)
              (assoc-in [:generated :status] :failed)
              (update :topic-seq rest))))
      (-> context
          (dissoc :generated)
          (assoc-in [:generated :status] :throttled)
          (update :topic-seq rest)))))

(defn maybe-backoff! [context attempts]
  (let [n-topics (count (keys (:generators context)))]
    (when (and (pos? attempts) (zero? (mod attempts n-topics)))
      (let [next-timestamps
            (reduce-kv
             (fn [all topic ts]
               (conj all (+ ts (get-in context [:generators topic :throttle-ns]))))
             []
             (:timestamps context))
            earliest-ts (apply min next-timestamps)
            now (System/nanoTime)
            delta-ms (* (max 0 (- earliest-ts now)) 0.000001)]
        (Thread/sleep delta-ms)))))

(defn advance-until-success [context]
  (loop [new-context (advance-step context)
         failed-attempts 0
         throttled-attempts 0]
    (if (> failed-attempts max-failed-attempts)
      (throw (ex-info "Couldn't generate another event. State machine may be livelocked."
                      {:context context}))
      (let [status (get-in new-context [:generated :status])]
        (cond (= status :success)
              new-context

              (= status :failed)
              (recur (advance-step new-context) (inc failed-attempts) throttled-attempts)

              (= status :throttled)
              (do (maybe-backoff! context throttled-attempts)
                  (recur (advance-step new-context) failed-attempts (inc throttled-attempts))))))))
