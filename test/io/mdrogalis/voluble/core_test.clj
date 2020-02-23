(ns io.mdrogalis.voluble.core-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.string :as s]
            [com.gfredericks.test.chuck.clojure-test :refer [checking]]
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
         (if (= x :solo)
           (assoc (get topics i) ns* :solo)
           (assoc-in (get topics i) [ns* :attrs] x)))
       kinds)))
   (gen/vector
    (gen/one-of [(gen/return :solo) gen-attr-names])
    (count topics))))

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
           vs (flatten-ns base topic :val)]
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
   (gen/vector (gen/one-of [(gen/return :key) (gen/return :val)]) (count attrs))))

(defn choose-dep-attr [by-topic attrs]
  (gen/fmap
   (fn [indices]
     (vec
      (map-indexed
       (fn [i n]
         (let [attr (get attrs i)]
           (if (:dep attr)
             (let [kind (get-in by-topic [(:dep attr) (:dep-ns attr)])]
               (if (= kind :solo)
                 (assoc attr :dep-attr nil)
                 (let [k (mod n (count (:attrs kind)))]
                   (assoc attr :dep-attr (get (:attrs kind) k)))))
             attr)))
       indices)))
   (gen/vector gen/large-integer (count attrs))))

(defn make-directive [attr]
  (cond (= (:key attr) :solo) "genkp"
        (= (:val attr) :solo) "genvp"
        (string? (:key attr)) "genk"
        (string? (:val attr)) "genp"
        :else (throw (ex-info "Couldn't make directive for attr." {:attr attr}))))

(defn make-attr-name [attr]
  (let [k (:key attr)
        v (:val attr)]
    (when (not (or (= k :solo) (= v :solo)))
      (or k v))))

(defn make-generator [attr]
  (if (:dep attr)
    "matching"
    "with"))

(defn make-prop-key [attr]
  (let [directive (make-directive attr)
        topic (:topic-name attr)
        attr-name (make-attr-name attr)
        generator (make-generator attr)
        parts (filter (comp not nil?) [directive topic attr-name generator])]
    (s/join "." parts)))

(defn resolve-dep-ns [ns*]
  (case ns*
    :key "key"
    :val "value"))

(defn make-prop-val [attr]
  (if (:dep attr)
    (let [ns* (resolve-dep-ns (:dep-ns attr))
          parts (filter (comp not nil?) [(:dep attr) ns* (:dep-attr attr)])]
      (s/join "." parts))
    (rand-nth expressions)))

(defn construct-props [attrs]
  (reduce
   (fn [all attr]
     (let [k (make-prop-key attr)
           v (make-prop-val attr)]
       (assoc all k v)))
   {}
   attrs))

(defn generate-props []
  (let [dag (gen-topic-dag)]
    (gen/let [flattened (flatten-dag dag)
              with-keys (choose-kv-kind flattened :key)
              with-vals (choose-kv-kind with-keys :val)]
      (let [by-topic (index-by-topic with-vals)
            attrs (flatten-attrs with-vals)]
        (gen/let [with-deps (choose-deps attrs)
                  with-dep-ns (choose-dep-ns with-deps)
                  with-dep-attr (choose-dep-attr by-topic with-dep-ns)]
          (let [attrs (dissoc-dep-choices with-dep-attr)
                kvs (construct-props attrs)]
            kvs))))))


(comment
  (clojure.pprint/pprint
   (tc/quick-check
    10
    (prop/for-all
     [props (generate-props)]
     (let [context (atom (c/make-context (merge props {"global.history.records.max" "5"})))
           records (atom [])
           iterations 100]

       (doseq [_ (range iterations)]
         (swap! context c/advance-until-success)
         (swap! records conj (:generated @context)))

       (= (count @records) iterations)))))

  )

;; 2. test sometimes

;; Props:
;; - Every record has a topic that was defined already
;; - :solo values are scalars, not maps
;; - Non-solos are maps
;; - undefined keys are nil
;; - undefined vals are nil
;; - history never exceeds global max
;; - history never exceeds topic max if it's set





(deftest primitive-keys
  (let [props {"genkp.users.with" "#{Name.male_first_name}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest complex-keys
  (let [props {"genk.users.name.with" "#{Name.male_first_name}"
               "genk.users.bloodType.with" "#{Name.blood_group}"
               "genk.users.creditCardNumber.with" "#{Finance.credit_card}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest primitive-values
  (let [props {"genvp.users.with" "#{Name.male_first_name}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint step)))

(deftest complex-values
  (let [props {"genv.users.name.with" "#{Name.male_first_name}"
               "genv.users.bloodType.with" "#{Name.blood_group}"
               "genv.users.creditCardNumber.with" "#{Finance.credit_card}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint step)))

(deftest prim-kvs
  (let [props {"genkp.users.with" "#{Name.male_first_name}"
               "genvp.users.with" "#{Name.blood_group}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint step)))



   




(deftest complex-kvs
  (let [props {"genk.users.name.with" "#{Name.male_first_name}"
               "genk.users.bloodType.with" "#{Name.blood_group}"
               "genv.users.creditCardNumber.with" "#{Finance.credit_card}"
               "genv.diets.dish.with" "#{Food.vegetables}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest matching-primitive-keys
  (let [props {"genkp.users.with" "#{Name.full_name}"
               "genvp.users.with" "#{Name.blood_group}"

               "genkp.publications.matching" "users.key"
               "genv.publications.bloodType.matching" "users.value"
               "genv.publications.title.with" "#{Book.title}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest matching-complex-keys
  (let [props {"genk.users.name.with" "#{Name.full_name}"
               "genv.users.bloodType.with" "#{Name.blood_group}"

               "genkp.publications.matching" "users.key.name"
               "genv.publications.bloodType.matching" "users.value.bloodType"
               "genv.publications.title.with" "#{Book.title}"}
        context (c/make-context props)]
    (clojure.pprint/pprint context)))

(deftest mutable-primitive-keys
  (let [props {"genkp.users.sometimes.with" "#{Name.full_name}"
               "genkp.users.sometimes.matching" "users.key"
               "genv.users.bloodType.with" "#{Name.blood_group}"}
        context (c/make-context props)]
    (clojure.pprint/pprint context)))

(deftest mutable-complex-keys
  (let [props {"genk.users.name.sometimes.with" "#{Name.full_name}"
               "genk.users.name.sometimes.matching" "users.key.name"
               "genv.users.bloodType.with" "#{Name.blood_group}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest attr-adjustable-mutation-rate
  (let [props {"genk.users.name.sometimes.with" "#{Name.full_name}"
               "genk.users.name.sometimes.matching" "users.key.name"
               "attrk.users.name.matching.rate" "0.5"

               "genv.users.bloodType.with" "#{Name.blood_group}"}]))

(deftest global-adjustable-mutation-rate
  (let [props {"global.matching.rate" "0.6"

               "genk.users.name.sometimes.with" "#{Name.full_name}"
               "genk.users.name.sometimes.matching" "users.key.name"
               "genv.users.bloodType.with" "#{Name.blood_group}"

               "genk.cats.name.sometimes.with" "#{Name.full_name}"
               "genk.cats.name.sometimes.matching" "cats.key.name"
               "genv.cats.bloodType.with" "#{Name.blood_group}"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest three-way-matching
  (let [props {"genkp.users.with" "#{Name.full_name}"
               "genv.users.credit_card_number.with" "#{Finance.credit_card}"

               "genk.cats.name.with" "#{FunnyName.name}"
               "genv.cats.owner.matching" "users.key"

               "genv.diets.catName.matching" "cats.key.name"
               "genv.diets.dish.with" "#{Food.vegetables}"
               "genv.diets.size.with" "#{Food.measurement_sizes}"
               "genv.diets.measurement.with" "#{Food.measurements}"}]))

(deftest attr-nullable-rates
  (let [props {"genk.users.name.with" "#{Name.male_first_name}"
               "genk.users.bloodType.with" "#{Name.blood_group}"
               "genv.users.creditCardNumber.with" "#{Finance.credit_card}"
               "attrv.users.creditCardNumber.null.rate" "0.5"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest topic-tombstone-rates
  (let [props {"genk.users.name.with" "#{Name.male_first_name}"
               "genv.users.bloodType.with" "#{Name.blood_group}"

               "topic.users.tombstone.rate" "0.8"}
        context (c/make-context props)
        step (c/advance-step context)]
    (clojure.pprint/pprint (:generated step))))

(deftest topic-history-purge-strategy
  (let [props {"genk.users.name.with" "#{Name.male_first_name}"
               "genv.users.bloodType.with" "#{Name.blood_group}"

               "topic.users.history.records.max" "1000"}]))

(deftest global-history-purge-strategy
  (let [props {"global.history.records.max" "1000"

               "genk.users.name.with" "#{Name.male_first_name}"
               "genv.users.bloodType.with" "#{Name.blood_group}"

               "genk.cats.name.with" "#{Name.full_name}"
               "genv.cats.bloodType.with" "#{Name.blood_group}"}
        context (c/make-context props)]
    (clojure.pprint/pprint context)))



;; (deftest parse-key
;;   (let [k "genkp.users.with"]
;;     (is (= {:kind :primitive-key
;;             :original-key k
;;             :topic "users"
;;             :qualified? false
;;             :generator "with"}
;;            (c/try-parse-key k))))
  
;;   (let [k "genk.users.name.with"]
;;     (is (= {:kind :complex-key
;;             :original-key k
;;             :topic "users"
;;             :attr "name"
;;             :qualified? false
;;             :generator "with"}
;;            (c/try-parse-key k))))

;;   (let [k "genvp.users.with"]
;;     (is (= {:kind :primitive-value
;;             :original-key k
;;             :topic "users"
;;             :qualified? false
;;             :generator "with"}
;;            (c/try-parse-key k))))

;;   (let [k "genv.cats.food.matching"]
;;     (is (= {:kind :complex-value
;;             :original-key k
;;             :topic "cats"
;;             :attr "food"
;;             :qualified? false
;;             :generator "matching"}
;;            (c/try-parse-key k))))

;;   (let [k "genv.users.age.sometimes.with"]
;;     (is (= {:kind :complex-value
;;             :original-key k
;;             :topic "users"
;;             :attr "age"
;;             :qualified? true
;;             :generator "with"}
;;            (c/try-parse-key k))))

;;   (let [k "genk.cats.food.sometimes.matching"]
;;     (is (= {:kind :complex-key
;;             :original-key k
;;             :topic "cats"
;;             :attr "food"
;;             :qualified? true
;;             :generator "matching"}
;;            (c/try-parse-key k)))))

;; ;; (deftest parse-value
;; ;;   (let [k (c/parse-prop-key "gen.users.name.with")]
;; ;;     (is (= {:strategy :isolated
;; ;;             :qualified? false
;; ;;             :expression "#{Name.full_name}"}
;; ;;            (c/parse-prop-value k "#{Name.full_name}"))))

;; ;;   (let [k (c/parse-prop-key "gen.users.name.matching")]
;; ;;     (is (= {:strategy :dependent
;; ;;             :qualified? false
;; ;;             :topic "users"
;; ;;             :attr "name"}
;; ;;            (c/parse-prop-value k "users.name"))))

;; ;;   (let [k (c/parse-prop-key "gen.users.name.xx")]
;; ;;     (is (thrown? IllegalArgumentException (c/parse-prop-value k "whatever")))))
