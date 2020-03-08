(ns io.mdrogalis.voluble.property-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.string :as s]
            [com.theinternate.generators.graph :as graph]
            [io.mdrogalis.voluble.core :as c]))

(def expressions
  ["#{Name.male_first_name}"
   "#{Name.female_first_name}"
   "#{Name.first_name}"
   "#{Name.last_name}"
   "#{Name.name}"
   "#{Name.name_with_middle}"
   "#{Name.prefix}"
   "#{Name.suffix}"
   "#{Name.title.descriptor}"
   "#{Name.title.level}"
   "#{Name.title.job}"
   "#{Name.blood_group}"
   "#{Address.city_prefix}"
   "#{Address.city_suffix}"
   "#{Address.country}"
   "#{Address.country_code}"
   "#{Address.country_code_long}"
   "#{Address.building_number}"
   "#{Address.community_prefix}"
   "#{Address.community_suffix}"
   "#{Address.street_suffix}"
   "#{Address.secondary_address}"
   "#{Address.postcode}"
   "#{Address.state}"
   "#{Address.state_abbr}"
   "#{Address.time_zone}"
   "#{Finance.credit_card}"
   "#{number.number_between '-999999999','999999999'}"
   "#{number.number_between '0','99'}.#{number.number_between '0','99'}"
   "#{bothify '????????','false'}"])

(def gen-attr-name
  (gen/such-that not-empty gen/string-alphanumeric))

(def gen-attr-names
  (gen/vector-distinct gen-attr-name {:min-elements 1}))

(defn gen-topic-dag []
  (gen/let
      [topic-names (gen/vector-distinct
                    (gen/such-that
                     not-empty
                     gen/string-alphanumeric)
                    {:min-elements 1})]
    (graph/gen-directed-acyclic-graph topic-names)))

(defn flatten-dag [dag-gen]
  (gen/fmap
   (fn [dag]
     (mapv
      (fn [[topic-name deps]]
        {:topic-name topic-name :deps (vec deps)})
      dag))
   dag-gen))

(defn choose-topic-bounds [topics]
  (gen/fmap
   (fn [caps]
     (reduce
      (fn [all [n i]]
        (if n
          (let [topic-name (get-in topics [i :topic-name])]
            (assoc-in all [topic-name :records-exactly] n))
          all))
      {}
      (map vector caps (range))))
   (gen/vector
    (gen/frequency [[8 (gen/return nil)] [2 (gen/large-integer* {:min 1})]])
    (count topics))))

(defn gen-global-configs []
  (gen/hash-map
   :max-history (gen/frequency [[8 (gen/return nil)] [2 (gen/large-integer* {:min 1})]])
   :matching-rate (gen/double* {:min 0.00000001 :max 1 :NaN? false :infinite? false})))

(defn choose-max-history [topics configs]
  (gen/fmap
   (fn [caps]
     (reduce
      (fn [all [n i]]
        (if n
          (let [topic-name (get-in topics [i :topic-name])]
            (assoc-in all [topic-name :max-history] n))
          all))
      configs
      (map vector caps (range))))
   (gen/vector
    (gen/frequency [[8 (gen/return nil)] [2 (gen/large-integer* {:min 1})]])
    (count topics))))

(defn choose-kv-kind [topics ns*]
  (gen/fmap
   (fn [kinds]
     (vec
      (map-indexed
       (fn [i x]
         (cond (= x :none)
               (get topics i)

               (= x :solo)
               (assoc (get topics i) ns* :solo)

               :else
               (assoc-in (get topics i) [ns* :attrs] x)))
       kinds)))
   (gen/vector
    (gen/one-of [(gen/return :none) (gen/return :solo) gen-attr-names])
    (count topics))))

(defn remove-orphan-topics [topics]
  (let [orphans (->> topics
                     (filter (fn [t] (and (nil? (:key t)) (nil? (:value t)))))
                     (map :topic-name)
                     (into #{}))]
    (->> topics
         (remove (fn [t] (contains? orphans (:topic-name t))))
         (map (fn [t] (update t :deps (fn [deps] (vec (remove (fn [d] (contains? orphans d)) deps))))))
         (vec))))

(defn index-by-topic [topics]
  (reduce-kv
   (fn [all k vs]
     (assoc all k (first vs)))
   {}
   (group-by :topic-name topics)))

(defn flatten-ns [base topic ns*]
  (let [kind (get topic ns*)]
    (if (= kind :solo)
      [(merge base {ns* :solo})]
      (mapv (fn [attr] (merge base {ns* attr})) (:attrs kind)))))

(defn flatten-attrs [topics]
  (reduce
   (fn [all topic]
     (let [base (select-keys topic [:topic-name :deps])
           ks (flatten-ns base topic :key)
           vs (flatten-ns base topic :value)]
       (into all (into ks vs))))
   []
   topics))

(defn choose-deps [attrs]
  (gen/fmap
   (fn [indices]
     (vec
      (map-indexed
       (fn [i n]
         (let [attr (get attrs i)]
           (if (or (not (seq (:deps attr))) (= n :none))
             (assoc attr :dep nil)
             (let [k (mod n (count (:deps attr)))]
               (assoc attr :dep (get (:deps attr) k))))))
       indices)))
   (gen/vector (gen/one-of [(gen/return :none) gen/large-integer]) (count attrs))))

(defn dissoc-dep-choices [attrs]
  (map #(dissoc % :deps) attrs))

(defn choose-dep-ns [attrs]
  (gen/fmap
   (fn [namespaces]
     (vec
      (map-indexed
       (fn [i ns*]
         (let [attr (get attrs i)]
           (if (:dep attr)
             (assoc attr :dep-ns ns*)
             attr)))
       namespaces)))
   (gen/vector (gen/one-of [(gen/return :key) (gen/return :value)]) (count attrs))))

(defn choose-dep-attr [by-topic attrs]
  (gen/fmap
   (fn [indices]
     (vec
      (map-indexed
       (fn [i n]
         (let [attr (get attrs i)]
           (if (:dep attr)
             (let [kind (get-in by-topic [(:dep attr) (:dep-ns attr)])]
               (if (or (= kind :solo) (empty? (:attrs kind)))
                 (assoc attr :dep-attr nil)
                 (let [k (mod n (count (:attrs kind)))]
                   (assoc attr :dep-attr (get (:attrs kind) k)))))
             attr)))
       indices)))
   (gen/vector gen/large-integer (count attrs))))

(defn choose-qualifier [attrs]
  (gen/fmap
   (fn [qualifiers]
     (vec
      (map-indexed
       (fn [i qualifier]
         (let [attr (get attrs i)]
           (if (and (:dep attr) qualifier)
             (assoc attr :qualifier :sometimes)
             attr)))
       qualifiers)))
   (gen/vector (gen/one-of [(gen/return true) (gen/return false)]) (count attrs))))

(defn make-directive [attr]
  (cond (= (:key attr) :solo) "genkp"
        (= (:value attr) :solo) "genvp"
        (string? (:key attr)) "genk"
        (string? (:value attr)) "genv"
        :else (throw (ex-info "Couldn't make directive for attr." {:attr attr}))))

(defn make-attr-name [attr]
  (let [k (:key attr)
        v (:value attr)]
    (when (not (or (= k :solo) (= v :solo)))
      (or k v))))

(defn make-qualifier [attr]
  (when (= (:qualifier attr) :sometimes)
    "sometimes"))

(defn make-generator [attr]
  (if (:dep attr)
    "matching"
    "with"))

(defn make-prop-key [attr]
  (let [directive (make-directive attr)
        topic (:topic-name attr)
        attr-name (make-attr-name attr)
        qualifier (make-qualifier attr)
        generator (make-generator attr)
        parts (filter (comp not nil?) [directive topic attr-name qualifier generator])]
    (s/join "." parts)))

(defn resolve-dep-ns [ns*]
  (case ns*
    :key "key"
    :value "value"))

(defn make-prop-val [attr]
  (if (:dep attr)
    (let [ns* (resolve-dep-ns (:dep-ns attr))
          parts (filter (comp not nil?) [(:dep attr) ns* (:dep-attr attr)])]
      (s/join "." parts))
    (rand-nth expressions)))

(defn make-props [attr]
  (if (= (:qualifier attr) :sometimes)
    (let [without-dep (dissoc attr :dep)
          k1 (make-prop-key attr)
          v1 (make-prop-val attr)
          k2 (make-prop-key without-dep)
          v2 (make-prop-val without-dep)]
      {k1 v1
       k2 v2})
    {(make-prop-key attr) (make-prop-val attr)}))

(defn construct-gen-props [attrs]
  (reduce
   (fn [all attr]
     (let [m (make-props attr)]
       (merge all m)))
   {}
   attrs))

(defmulti construct-topic-config-kv
  (fn [topic k v]
    k))

(defmethod construct-topic-config-kv :records-exactly
  [topic k v]
  {(format "topic.%s.records.exactly" topic) (str v)})

(defmethod construct-topic-config-kv :max-history
  [topic k v]
  {(format "topic.%s.history.records.max" topic) (str v)})

(defn construct-topic-props [attrs]
  (reduce-kv
   (fn [all topic configs]
     (reduce-kv
      (fn [all* k v]
        (merge all* (construct-topic-config-kv topic k v)))
      all
      configs))
   {}
   attrs))

(defmulti construct-global-config-kv
  (fn [k v]
    k))

(defmethod construct-global-config-kv :max-history
  [k v]
  {"global.history.records.max" (str v)})

(defmethod construct-global-config-kv :matching-rate
  [k v]
  {"global.matching.rate" (str v)})

(defn construct-global-props [attrs]
  (reduce-kv
   (fn [all k v]
     (if v
       (merge all (construct-global-config-kv k v))
       all))
   {}
   attrs))

(defn generate-props []
  (let [dag (gen-topic-dag)]
    (gen/let [flattened (flatten-dag dag)
              with-keys (choose-kv-kind flattened :key)
              with-vals (choose-kv-kind with-keys :value)]
      (let [without-orphans (remove-orphan-topics with-vals)
            by-topic (index-by-topic without-orphans)
            flat-attrs (flatten-attrs without-orphans)]
        (gen/let [topic-configs (choose-topic-bounds without-orphans)
                  topic-configs (choose-max-history without-orphans topic-configs)
                  global-configs (gen-global-configs)
                  with-deps (choose-deps flat-attrs)
                  with-dep-ns (choose-dep-ns with-deps)
                  with-dep-attr (choose-dep-attr by-topic with-dep-ns)
                  with-qualifier (choose-qualifier with-dep-attr)]
          (let [attrs (dissoc-dep-choices with-qualifier)
                kvs (merge (construct-gen-props attrs)
                           (construct-topic-props topic-configs)
                           (construct-global-props global-configs))]
            {:props kvs
             :topics (into #{} (keys by-topic))
             :topic-configs topic-configs
             :global-configs global-configs
             :attrs attrs
             :by-topic by-topic}))))))

(defn validate-data-type [by-topic event ns*]
  (let [expected (get-in by-topic [(:topic event) ns*])
        actual (get-in event [:event ns*])]
    (cond (= expected :solo)
          (is (not (coll? actual)))

          (map? expected)
          (is (coll? actual))

          (nil? expected)
          (is (nil? actual))

          :else
          (throw (ex-info "Data type was an unexpected."
                          {:expected expected :actual actual})))))

(defn index-by-attribute [ns->attrs]
  (reduce-kv
   (fn [all ns* attrs]
     (let [by-attr (group-by (fn [attr] (or (:key attr) (:value attr))) attrs)
           as-attr (into {} (map (fn [[k v]] [k (first v)]) by-attr))]
       (assoc all ns* as-attr)))
   {}
   ns->attrs))

(defn build-attributes-index [attrs]
  (let [topic->attrs (group-by :topic-name attrs)]
    (reduce-kv
     (fn [all k vs]
       (let [ns->attrs (group-by (fn [v] (if (:key v) :key :value)) vs)
             ns->attr (index-by-attribute ns->attrs)]
         (assoc all k ns->attr)))
     {}
     topic->attrs)))

(defn validate-attribute-dependency [event-index attr x]
  (when (:dep attr)
    (let [ks (filter (comp not nil?) [(:dep attr) (:dep-ns attr) (:dep-attr attr)])
          target (get-in event-index ks)]
      (when (and (not (and (nil? target) (nil? x)))
                 (not= (:qualifier attr) :sometimes))
        (is (contains? target x))))))

(defn validate-dependencies [indexed-attrs event-index ns* event]
  (let [t (:topic event)
        x (get-in event [:event ns*])]
    (if (coll? x)
      (doseq [[k v] x]
        (let [attr (get-in indexed-attrs [t ns* k])]
          (validate-attribute-dependency event-index attr v)))
      (let [attr (get-in indexed-attrs [t ns* :solo])]
        (validate-attribute-dependency event-index attr x)))))

(defn index-event [index ns* event]
  (let [t (:topic event)
        x (get-in event [:event ns*])]
    (if (coll? x)
      (reduce-kv
       (fn [i k v]
         (update-in i [t ns* k] (fnil conj #{}) v))
       index
       x)
      (update-in index [t ns*] (fnil conj #{}) x))))

(defn remove-drained [events]
  (remove (fn [event] (= (:status event) :drained)) events))

(defn only-bounded-topics [topic-configs]
  (->> topic-configs
       (keep (fn [[topic config]] (when (:records-exactly config) topic)))
       (into #{})))

(defn expected-records [topics topic-configs iterations]
  (if (= (into #{} topics) (only-bounded-topics topic-configs))
    (min iterations (apply + (map :records-exactly (vals topic-configs))))
    iterations))

(defn validate-history! [context global-configs topic-configs]
  (doseq [[topic topic-config] topic-configs]
    (let [n (or (:max-history topic-config) (:max-history global-configs))]
      (when n
        (is (<= (count (get-in context [:history topic])) n))))))

(defspec property-test
  150
  (prop/for-all
   [{:keys [props topics topic-configs global-configs attrs by-topic]} (generate-props)]
   (if (not (empty? props))
     (let [context (atom (c/make-context props))
           records (atom [])
           topic-count (atom {})
           iterations 500]

       (doseq [_ (range iterations)]
         (swap! context c/advance-until-success)
         (let [state @context]
           (swap! records conj (:generated state))

           ;; History sizes never outgrow their max.
           (validate-history! state global-configs topic-configs)))

       (let [events (remove-drained @records)
             event-index (atom {})
             indexed-attrs (build-attributes-index attrs)]
         
         ;; It doesn't livelock.
         (is (= (count events)
                (expected-records (keys by-topic) topic-configs iterations)))

         (doseq [event events]
           ;; Every record is generated for a topic in the props.
           (is (contains? topics (:topic event)))

           ;; Solo keys are scalar, complex keys are maps.
           (validate-data-type by-topic event :key)

           ;; Ditto values.
           (validate-data-type by-topic event :value)

           ;; Dependent values pre-exist in the right collection.
           (validate-dependencies indexed-attrs @event-index :key event)
           (validate-dependencies indexed-attrs @event-index :value event)

           (swap! event-index index-event :key event)
           (swap! event-index index-event :value event)

           (swap! topic-count update (:topic event) (fnil inc 0)))

         ;; Bounded topics have exactly set don't exceed their count.
         (let [topic-count-state @topic-count]
           (doseq [[topic {:keys [records-exactly]}] topic-configs]
             (when records-exactly
               (is (<= (get topic-count-state topic) records-exactly)))))
         true))
     true)))

;; Future props:
;; - nil-ish vals
;; - tombstones
;; - attr matching rates
 

#_(clojure.test/run-tests)
