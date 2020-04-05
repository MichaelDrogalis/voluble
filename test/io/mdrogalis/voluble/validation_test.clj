(ns io.mdrogalis.voluble.validation-test
  (:require [clojure.test :refer :all]
            [io.mdrogalis.voluble.core :as c]))

(defn expect-exception! [props]
  (try
    (c/make-context props)
    (is false)
    (catch IllegalArgumentException e
      (is (not (nil? (.getMessage e)))))))

(deftest unused-topic-config
  (let [props {"genkp.t1.with" "#{Beer.name}"
               "genv.t1.card_type.with" "#{Business.creditCardType}"
               "topic.t2.records.exactly" "10"}]
    (expect-exception! props)))

(deftest undefined-attr-config
  (let [props {"genkp.t1.with" "#{Beer.name}"
               "genv.t1.card_type.with" "#{Business.creditCardType}"
               "attrkp.t1.something" "10"}]
    (expect-exception! props))

  (let [props {"genkp.t1.with" "#{Beer.name}"
               "genv.t1.card_type.with" "#{Business.creditCardType}"
               "attrv.t1.card_type.something" "10"}]
    (expect-exception! props))

  (let [props {"genk.t1.beer.with" "#{Beer.name}"
               "genv.t1.card_type.with" "#{Business.creditCardType}"
               "attrk.t1.beer.something" "10"}]
    (expect-exception! props))

  (let [props {"genk.t1.beer.with" "#{Beer.name}"
               "genvp.t1.card_type.with" "#{Business.creditCardType}"
               "attrvp.t1.card_type.something" "10"}]
    (expect-exception! props)))

(deftest bad-attr-shape
  (let [props {"genkp.t1.with" "#{Beer.name}"
               "attrk.t1.a1.null.rate" "10"}]
    (expect-exception! props))

  (let [props {"genk.t1.a1.with" "#{Beer.name}"
               "attrkp.t1.null.rate" "10"}]
    (expect-exception! props))

  (let [props {"genvp.t1.with" "#{Beer.name}"
               "attrv.t1.a1.null.rate" "10"}]
    (expect-exception! props))

  (let [props {"genv.t1.a1.with" "#{Beer.name}"
               "attrvp.t1.null.rate" "10"}]
    (expect-exception! props)))

(deftest unused-attrs
  (let [props {"genk.t1.a1.with" "#{Beer.name}"
               "attrk.t1.b1.null.rate" "10"}]
    (expect-exception! props))
  
  (let [props {"genv.t1.a1.with" "#{Beer.name}"
               "attrv.t1.b1.null.rate" "10"}]
    (expect-exception! props)))

(deftest shape-conflicts
  (let [props {"genkp.t1.with" "#{Beer.name}"
               "genk.t1.a1.with" "#{Beer.name}"}]
    (expect-exception! props))

  (let [props {"genvp.t1.with" "#{Beer.name}"
               "genv.t1.a1.with" "#{Beer.name}"}]
    (expect-exception! props)))

(deftest bad-dependencies
  (let [props {"genkp.t1.matching" "t2.key"}]
    (expect-exception! props))

  (let [props {"genkp.t1.matching" "t2.key"
               "genv.t2.a.with" "#{Beer.name}"}]
    (expect-exception! props))

  (let [props {"genkp.t1.matching" "t2.value"}]
    (expect-exception! props))

  (let [props {"genkp.t1.matching" "t2.value.b"
               "genv.t2.a.with" "#{Beer.name}"}]
    (expect-exception! props))

  (let [props {"genvp.t1.matching" "t2.value"}]
    (expect-exception! props))

  (let [props {"genvp.t1.matching" "t2.value"
               "genk.t2.a.with" "#{Beer.name}"}]
    (expect-exception! props))

  (let [props {"genvp.t1.matching" "t2.key"}]
    (expect-exception! props))

  (let [props {"genvp.t1.matching" "t2.key.b"
               "genk.t2.a.with" "#{Beer.name}"}]
    (expect-exception! props)))
