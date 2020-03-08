(ns io.mdrogalis.voluble.parse
  (:require [clojure.string :as s]))

(def namespaces
  {"genkp" :key
   "genk" :key
   "genvp" :value
   "genv" :value
   "attrkp" :key
   "attrk" :key
   "attrvp" :value
   "attrv" :value})

(defmulti parse-key
  (fn [kind k]
    kind))

(defmethod parse-key :generate-primitive
  [kind k]
  (let [;; (genkp|genvp).<topic>[.sometimes].(with|matching)
        expr #"(genkp|genvp)\.([^\.]+?)(\.sometimes)?\.([^\.]+?)"
        [k-or-v topic qualified? generator] (rest (re-matches expr k))
        ns* (get namespaces k-or-v)]
    (when topic
      {:kind kind
       :original-key k
       :topic topic
       :ns ns*
       :qualified? (not (nil? qualified?))
       :generator generator})))

(defmethod parse-key :generate-complex
  [kind k]
  (let [;; (genk|genv).<topic>.<attr>[.sometimes].(with|matching)
        expr #"(genk|genv)\.([^\.]+?)\.([^\.]+?)(\.sometimes)?\.([^\.]+?)"
        [k-or-v topic attr qualified? generator] (rest (re-matches expr k))
        ns* (get namespaces k-or-v)]
    (when topic
      {:kind kind
       :original-key k
       :topic topic
       :ns ns*
       :attr attr
       :qualified? (not (nil? qualified?))
       :generator generator})))

(defmethod parse-key :attribute-primitive
  [kind k]
  (let [;; (attrkp|attrvp).<topic>.[props+]
        expr #"(attrkp|attrvp)\.([^\.]+?)\.(.+?)"
        [k-or-v topic unparsed-config] (rest (re-matches expr k))
        ns* (get namespaces k-or-v)]
    (when unparsed-config
      {:kind kind
       :original-key k
       :topic topic
       :ns ns*
       :config (s/split unparsed-config #"\.")})))

(defmethod parse-key :attribute-complex
  [kind k]
  (let [;; (attrk|attrv).<topic>.<attr>.[props+]
        expr #"(attrk|attrv)\.([^\.]+?)\.([^\.]+?)\.(.+?)"
        [k-or-v topic attr unparsed-config] (rest (re-matches expr k))
        ns* (get namespaces k-or-v)]
    (when unparsed-config
      {:kind kind
       :original-key k
       :topic topic
       :ns ns*
       :attr attr
       :config (s/split unparsed-config #"\.")})))

(defmethod parse-key :topic
  [kind k]
  (let [;; topic.<topic>.[props+]
        expr #"topic\.([^\.]+?)\.(.+?)"
        [topic unparsed-config] (rest (re-matches expr k))]
    (when unparsed-config
      {:kind kind
       :original-key k
       :topic topic
       :config (s/split unparsed-config #"\.")})))

(defmethod parse-key :global
  [kind k]
  (let [;; global.<topic>.[props+]
        expr #"global\.(.+?)"
        [unparsed-config] (rest (re-matches expr k))]
    (when unparsed-config
      {:kind kind
       :original-key k
       :config (s/split unparsed-config #"\.")})))

(defn try-parse-key [k]
  (reduce-kv
   (fn [_ kind f]
     (let [result (f kind k)]
       (when result
         (reduced result))))
   nil
   (methods parse-key)))

(defn parse-generator-value [{:keys [generator]} v]
  (case generator
    "with" {:strategy :isolated
            :expression v}
    "matching" (let [[topic ns* attr] (s/split v #"\.")]
                 {:strategy :dependent
                  :topic topic
                  :ns (keyword ns*)
                  :attr attr})))

(defn parse-global-value [{:keys [config] :as parsed-k} v]
  (cond (= config ["history" "records" "max"])
        (Long/parseLong v)

        (= config ["matching" "rate"])
        (Double/parseDouble v)

        (= config ["throttle" "ms"])
        (Long/parseLong v)

        :else v))

(defn parse-topic-value [{:keys [config] :as parsed-k} v]
  (cond (= config ["history" "records" "max"])
        (Long/parseLong v)

        (= config ["tombstone" "rate"])
        (Double/parseDouble v)

        (= config ["throttle" "ms"])
        (Long/parseLong v)

        (= config ["records" "exactly"])
        (let [n (Long/parseLong v)]
          (when (not (pos? n))
            (throw (ex-info "records.exactly must be greater than 0" {:value v})))
          n)

        :else v))

(defn parse-attr-value [{:keys [config] :as parsed-k} v]
  (cond (= config ["null" "rate"])
        (Double/parseDouble v)

        (= config ["matching" "rate"])
        (Double/parseDouble v)

        :else v))

(defn augment-parsed-val [parsed-k parsed-v]
  (let [sub-keys (select-keys parsed-k [:topic :ns :attr :original-key :qualified?])]
    (merge sub-keys parsed-v)))

(defn parse-keys [props]
  (reduce-kv
   (fn [all k v]
     (if-let [parsed-key (try-parse-key k)]
       (assoc all parsed-key v)
       all))
   {}
   props))
