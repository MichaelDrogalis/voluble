(ns io.mdrogalis.voluble.generate
  (:import [com.github.javafaker Faker]))

(def default-matching-rate 0.1)

(def default-null-rate 0.0)

(def default-tombstone-rate 0.0)

(defn choose-matching-rate [{:keys [attr-configs global-configs]} topic ns* attr]
  (let [topic-rate (get-in attr-configs (concat [topic ns*] attr ["matching" "rate"]))
        global-rate (get-in global-configs ["matching" "rate"])]
    (or topic-rate global-rate default-matching-rate)))

(defn choose-null-rate [{:keys [attr-configs]} topic ns* attr]
  (get-in attr-configs (concat [topic ns*] attr ["null" "rate"]) default-null-rate))

(defn nullable [context {:keys [topic attr] :as generator} gen-f]
  (let [rate (choose-null-rate context topic (:ns generator) attr)]
    (if (zero? rate)
      gen-f
      (fn [deps]
        (when (>= (rand) rate)
          (gen-f deps))))))

(defmulti gen-value-fn
  (fn [context generator]
    (:strategy generator)))

(defmethod gen-value-fn :isolated
  [{:keys [faker] :as context} generator]
  (nullable context generator
            (fn [deps]
              (.expression ^Faker faker (:expression generator)))))

(defmethod gen-value-fn :dependent
  [context {:keys [topic attr] :as generator}]
  (nullable context generator
            (if attr
              (fn [deps]
                (get-in deps (into [topic (:ns generator)] attr)))
              (fn [deps]
                (get-in deps [topic (:ns generator)])))))

(defmethod gen-value-fn :either
  [context {:keys [topic attr expression] :as generator}]
  (let [rate (choose-matching-rate context topic (:ns generator) attr)
        get-dep (if attr
                  (fn [deps] (get-in deps (into [topic (:ns generator)] attr)))
                  (fn [deps] (get-in deps [topic (:ns generator)])))]
    (nullable context generator
              (fn [deps]
                (let [dep (get-dep deps)]
                  (if (and (<= (rand) rate) dep)
                    dep
                    (.expression ^Faker (:faker context) expression)))))))

(defn gen-row-dependencies [context deps]
  (reduce
   (fn [all dep]
     (assoc all dep (rand-nth (get-in context [:history dep]))))
   {}
   deps))

(defmulti verify-deps-fn
  (fn [generator]
    (:strategy generator)))

(defmethod verify-deps-fn :isolated
  [generator]
  (constantly true))

(defmethod verify-deps-fn :dependent
  [generator]
  (fn [deps dep-targets]
    (and (not (empty? deps))
         (every? (comp not nil?) (vals dep-targets)))))

(defmethod verify-deps-fn :either
  [generator]
  (constantly true))

(defn invoke-generator [context deps key-gen-f val-gen-f]
  (let [dep-targets (gen-row-dependencies context deps)
        key-results (key-gen-f deps dep-targets)
        val-results (val-gen-f deps dep-targets)]
    {:key-results key-results
     :val-results val-results}))
