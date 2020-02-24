(ns io.mdrogalis.voluble.generate
  (:import [com.github.javafaker Faker]))

(def default-matching-rate 0.1)

(def default-null-rate 0.0)

(def default-tombstone-rate 0.0)

(defn choose-matching-rate [{:keys [attr-configs global-configs]} topic ns* attr]
  (let [topic-rate (get-in attr-configs [topic ns* attr "matching" "rate"])
        global-rate (get-in global-configs ["matching" "rate"])]
    (cond topic-rate (Double/parseDouble topic-rate)
          global-rate (Double/parseDouble global-rate)
          :else default-matching-rate)))

(defmulti gen-value
  (fn [context deps generator]
    (:strategy generator)))

(defmethod gen-value :isolated
  [{:keys [faker] :as context} deps generator]
  (.expression ^Faker faker (:expression generator)))

(defmethod gen-value :dependent
  [context deps {:keys [topic attr] :as generator}]
  (if attr
    (get-in deps [topic (:ns generator) attr])
    (get-in deps [topic (:ns generator)])))

(defmethod gen-value :either
  [context deps {:keys [topic attr expression] :as generator}]
  (let [rate (choose-matching-rate context topic (:ns generator) attr)
        dep (if attr
              (get-in deps [topic (:ns generator) attr])
              (get-in deps [topic (:ns generator)]))]
    (if (and (<= (rand) rate) dep)
      dep
      (.expression ^Faker (:faker context) expression))))

(defn choose-null-rate [{:keys [attr-configs]} topic ns* attr]
  (let [attr-rate (get-in attr-configs [topic ns* attr "null" "rate"])]
    (if attr-rate
      (Double/parseDouble attr-rate)
      default-null-rate)))

(defn gen-nullable [context deps {:keys [topic attr] :as generator}]
  (let [rate (choose-null-rate context topic (:ns generator) attr)]
    (when (>= (rand) rate)
      (gen-value context deps generator))))

(defn gen-row-dependencies [context deps]
  (reduce
   (fn [all dep]
     (assoc all dep (rand-nth (get-in context [:history dep]))))
   {}
   deps))

(defmulti can-gen-value?
  (fn [strategy deps dep-targets]
    strategy))

(defmethod can-gen-value? :isolated
  [strategy deps dep-targets]
  true)

(defmethod can-gen-value? :dependent
  [strategy deps dep-targets]
  (and (not (empty? deps))
       (some (comp not nil?) (vals dep-targets))))

(defmethod can-gen-value? :either
  [strategy deps dep-targets]
  true)

(defn invoke-generator [context deps generators]
  (let [dep-targets (gen-row-dependencies context deps)]
    (if-let [solo (:solo generators)]
      (if (can-gen-value? (:strategy solo) deps dep-targets)
        {:success? true
         :result (gen-nullable context dep-targets solo)}
        {:success? false})
      (reduce-kv
       (fn [all attr generator]
         (if (can-gen-value? (:strategy generator) deps dep-targets)
           (let [generated (gen-nullable context dep-targets generator)]
             (assoc-in all [:result attr] generated))
           (reduced {:success? false})))
       {:success? true
        :result nil}
       (:attrs generators)))))

(defn invoke-key-generator [context deps generators]
  (invoke-generator context deps generators))

(defn invoke-value-generator [context topic deps generators]
  (let [f (partial invoke-generator context deps generators)]
    (if-let [topic-rate (get-in context [:topic-configs topic "tombstone" "rate"])]
      (let [rate (Double/parseDouble topic-rate)]
        (if (>= (rand) rate)
          (f)
          {:success? true :value nil}))
      (f))))
