(ns io.mdrogalis.voluble.compile)

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

(comment
  (throw (IllegalArgumentException.
          
          (format "Invalid series of generators for collection '%s' and key '%s'. Expected a single 'within', a single 'matching', or a pair of 'sometimes.with' and 'sometimes.matching'. Found: %s"
                  (:collection (first generators))
                  (:column (first generators))
                  (s/join ", " (map :original-key generators))))))

(defn compile-gen-namespace [precompiled topic namespaces ns*]
  (let [attrs-gen (get-in namespaces [ns* :attrs])
        solo-gen (get-in namespaces [ns* :solo])]
    (cond attrs-gen
          (reduce-kv
           (fn [pre attr generators]
             (let [generator (compile-generator-strategy generators)]
               (assoc-in pre [topic ns* :attrs attr] generator)))
           precompiled
           attrs-gen)

          solo-gen
          (let [generator (compile-generator-strategy solo-gen)]
            (assoc-in precompiled [topic ns* :solo] generator))

          :else precompiled)))

(defn compile-generator-strategies [m]
  (reduce-kv
   (fn [all topic namespaces]
     (-> all
         (compile-gen-namespace topic namespaces :key)
         (compile-gen-namespace topic namespaces :value)))
   m
   m))
