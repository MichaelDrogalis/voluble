(ns io.mdrogalis.voluble.compile
  (:require [io.mdrogalis.voluble.generate :as g]))

(defn sort-strategies [strategies]
  (sort-by (juxt :strategy :qualified?) strategies))

(defn compile-generator-strategy [generators]
  (let [sorted-seq (sort-strategies generators)
        sorted-ks (map #(select-keys % [:strategy :qualified?]) sorted-seq)]
    (cond (= sorted-ks (sort-strategies [{:strategy :isolated :qualified? false}]))
          (first generators)

          (= sorted-ks (sort-strategies [{:strategy :dependent :qualified? false}]))
          (first generators)

          (= sorted-ks (sort-strategies [{:strategy :isolated :qualified? true}
                                         {:strategy :dependent :qualified? true}]))
          {:strategy :either
           :expression (:expression (second sorted-seq))
           :topic (:topic (first sorted-seq))
           :ns (:ns (first sorted-seq))
           :attr (:attr (first sorted-seq))})))

(defn compile-solo-gen [context verify-deps-f value-gen-f]
  (fn [deps dep-targets]
    (if (verify-deps-f deps dep-targets)
      {:success? true
       :result (value-gen-f dep-targets)}
      {:success? false})))

(defn compile-attrs-gen [context attr-fns]
  (fn [deps dep-targets]
    (reduce-kv
     (fn [all attr {:keys [verify-deps-f value-gen-f]}]
       (if (verify-deps-f deps dep-targets)
         (let [generated (value-gen-f dep-targets)]
           (assoc-in all [:result attr] generated))
         (reduced {:success? false})))
     {:success? true
      :result nil}
     attr-fns)))

(defn maybe-tombstone [context topic ns* gen-f]
  (let [topic-rate (get-in context [:topic-configs topic "tombstone" "rate"])]
    (if (and (= ns* :value) topic-rate)
      (fn [deps dep-targets]
        (if (>= (rand) topic-rate)
          (gen-f deps dep-targets)
          {:success? true :value nil}))
      gen-f)))

(defn compile-gen-namespace [context topic namespaces ns*]
  (let [solo-gen (get-in namespaces [ns* :solo])
        attrs-gen (get-in namespaces [ns* :attrs])]
    (cond solo-gen
          (let [generator (compile-generator-strategy solo-gen)
                verify-deps-f (g/verify-deps-fn generator)
                value-gen-f (g/gen-value-fn context generator)
                gen-f (compile-solo-gen context verify-deps-f value-gen-f)
                tombstone-f (maybe-tombstone context topic ns* gen-f)]
            (assoc-in context [:generators topic ns*] tombstone-f))

          attrs-gen
          (let [attr-fns (reduce-kv
                          (fn [all attr generators]
                            (let [generator (compile-generator-strategy generators)
                                  verify-deps-f (g/verify-deps-fn generator)
                                  gen-f (g/gen-value-fn context generator)]
                              (-> all
                                  (assoc-in [attr :verify-deps-f] verify-deps-f)
                                  (assoc-in [attr :value-gen-f] gen-f))))
                          {}
                          attrs-gen)
                gen-f (compile-attrs-gen context attr-fns)
                tombstone-f (maybe-tombstone context topic ns* gen-f)]
            (assoc-in context [:generators topic ns*] tombstone-f))

          :else context)))

(defn compile-generator-strategies [context]
  (reduce-kv
   (fn [ctx topic namespaces]
     (-> ctx
         (compile-gen-namespace topic namespaces :key)
         (compile-gen-namespace topic namespaces :value)))
   context
   (:generators context)))

(defn default-retire-fn [context]
  (update context :topic-seq rest))

(defn bounded-retire-fn [context topic n]
  (let [n-topics (count (keys (:generators context)))]
    (fn [rt-context]
      (let [current (get-in rt-context [:records-produced topic])]
        (if (>= current n)
          (let [retired-topics (count (:retired-topics rt-context))
                left (dec (- n-topics retired-topics))]
            (-> rt-context
                (update :topic-seq (fn [topics] (cycle (take left (rest topics)))))
                (update :retired-topics (fnil conj []) topic)))
          (default-retire-fn rt-context))))))

(defn compile-retire-strategy [context]
  (reduce
   (fn [ctx topic]
     (if-let [n (get-in context [:topic-configs topic "records" "exactly"])]
       (assoc-in ctx [:generators topic :retire-fn] (bounded-retire-fn ctx topic n))
       (assoc-in ctx [:generators topic :retire-fn] default-retire-fn)))
   context
   (keys (:generators context))))
