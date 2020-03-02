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
         (map (fn [t] (update t :deps (fn [deps] (vec (remove (fn [d] (contains? orphans d)) deps)))))))))

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

(defn construct-props [attrs]
  (reduce
   (fn [all attr]
     (let [m (make-props attr)]
       (merge all m)))
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
        (gen/let [with-deps (choose-deps flat-attrs)
                  with-dep-ns (choose-dep-ns with-deps)
                  with-dep-attr (choose-dep-attr by-topic with-dep-ns)
                  with-qualifier (choose-qualifier with-dep-attr)]
          (let [attrs (dissoc-dep-choices with-qualifier)
                kvs (construct-props attrs)]
            {:props kvs
             :topics (into #{} (keys by-topic))
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

(comment (property-test))

(defspec property-test
  100
  (prop/for-all
   [{:keys [props topics by-topic]} (generate-props)]
   (if (not (empty? props))
     (let [context (atom (c/make-context props))
           records (atom [])
           iterations 500]

       (doseq [_ (range iterations)]
         (swap! context c/advance-until-success)
         (swap! records conj (:generated @context)))

       (let [events @records]
         ;; It doesn't livelock.
         (is (= (count events) iterations))
         
         (doseq [event events]

           ;; Every record is generated for a topic in the props.
           (is (contains? topics (:topic event)))

           ;; Solo keys are scalar, complex keys are maps.
           (validate-data-type by-topic event :key)

           ;; Ditto values.
           (validate-data-type by-topic event :value))

         true))
     true)))



;; 2. test sometimes

;; Props:
;; - history never exceeds global max
;; - history never exceeds topic max if it's set
;; - matching always takes from the other topic
;; - integrate different global/topic/attr settings
