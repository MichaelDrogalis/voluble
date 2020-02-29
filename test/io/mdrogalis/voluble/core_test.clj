(ns io.mdrogalis.voluble.core-test
  (:require [clojure.test :refer :all]
            [io.mdrogalis.voluble.core :as c]))




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
    (clojure.pprint/pprint (:generated step))))





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
        context (c/make-context props)]
    (clojure.pprint/pprint (dissoc context :topic-seq))))



   




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
    ;(clojure.pprint/pprint context)
    ))

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

               "genv.users.bloodType.with" "#{Name.blood_group}"}
        context (c/make-context props)]
    (clojure.pprint/pprint context)))

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
    (clojure.pprint/pprint context)))

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
