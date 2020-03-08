(ns io.mdrogalis.voluble.interop
  (:require [io.mdrogalis.voluble.core :as c])
  (:import [java.util HashMap]
           [java.util ArrayList]
           [org.apache.kafka.connect.source SourceRecord]
           [org.apache.kafka.connect.data SchemaBuilder]
           [org.apache.kafka.connect.data Schema]
           [org.apache.kafka.connect.data Struct]))

(defn make-context [props]
  (atom (c/make-context (into {} props))))

(defn primitive-schema [t]
  (get
   {nil Schema/OPTIONAL_BYTES_SCHEMA
    java.lang.Integer Schema/OPTIONAL_INT32_SCHEMA
    java.lang.Long Schema/OPTIONAL_INT64_SCHEMA
    java.lang.Float Schema/OPTIONAL_FLOAT32_SCHEMA
    java.lang.Double Schema/OPTIONAL_FLOAT64_SCHEMA
    java.lang.String Schema/OPTIONAL_STRING_SCHEMA
    java.lang.Boolean Schema/OPTIONAL_BOOLEAN_SCHEMA}
   (type t)))

(defn complex-schema [x]
  (let [builder (.optional (SchemaBuilder/struct))]
    (.build
     ^SchemaBuilder
     (reduce-kv
      (fn [^SchemaBuilder b k v]
        (.field b k (primitive-schema v)))
      builder
      x))))

(defn build-schema [x]
  (cond (map? x)
        (complex-schema x)

        (not (nil? x))
        (primitive-schema x)))

(defn build-converted-obj [x schema]
  (if (map? x)
    (let [s (Struct. schema)]
      (doseq [[^String k v] x]
        (.put s k v))
      s)
    x))

(defn generate-source-record [state]
  (swap! state c/advance-until-success)
  (let [context @state
        generated (:generated context)
        status (:status generated)]
    (cond (= status :success)
          (let [records (ArrayList.)
                topic (get-in generated [:topic])
                k (get-in generated [:event :key])
                k-schema (build-schema k)
                k-obj (build-converted-obj k k-schema)
                v (get-in generated [:event :value])
                v-schema (build-schema v)
                v-obj (build-converted-obj v v-schema)
                record (SourceRecord. (HashMap.) (HashMap.) topic nil k-schema k-obj v-schema v-obj)]
            (.add records record)
            records)

          (= status :drained)
          (ArrayList.)

          :else
          (throw (ex-info "State machine returned an unusable status." {:status status})))))

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

(defn -main [& args]
  (let [props {"genk.RCkl.bCJf.matching" "VE.key",
               "genp.RCkl.E.with" "#{Address.community_prefix}",
               "genp.c.n08.matching" "VE.value",
               "genp.c.Vxq.with" "#{Address.community_suffix}",
               "genp.RCkl.5v.matching" "VE.key",
               "genp.RCkl.41.with" "#{Address.community_suffix}",
               "genk.RCkl.kD.matching" "VE.value",
               "genkp.VE.with" "#{Name.name_with_middle}",
               "genk.RCkl.94Y.with" "#{Name.title.level}",
               "genk.RCkl.W.with" "#{Name.male_first_name}",
               "genp.c.5D.matching" "VE.value",
               "genp.RCkl.nm.with" "#{Name.title.level}",
               "genp.c.KAz.matching" "VE.key",
               "genp.RCkl.C6S.matching" "VE.key",
               "genvp.VE.with" "#{Address.postcode}",
               "genkp.c.with" "#{Name.title.descriptor}"

               "global.throttle.ms" "250"}
        context (make-context props)
        f (com.github.javafaker.Faker.)]
    (doseq [_ (range 250)]
      (prn (generate-source-record context)))))
