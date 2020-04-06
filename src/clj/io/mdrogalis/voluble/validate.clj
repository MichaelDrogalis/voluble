(ns io.mdrogalis.voluble.validate
  (:require [clojure.set :as s]
            [clojure.string :refer [join]]))

(defn format-topics [topics]
  (join ", " topics))

(defn original-configs [context k topics]
  (mapcat
   (fn [t]
     (map (juxt :original-key :original-value)
          (get-in context [:configs-by-topic k t])))
   topics))

(defn original-gen-configs [context topic ns*]
  (map (juxt :original-key :original-value)
       (get-in context [:configs-by-topic :gen topic ns*])))

(defn format-attr [attr]
  (join "->" attr))

(defn ^String pretty-error-msg [msg bad-configs]
  (str
   (reduce
    (fn [body [k v]]
      (str body (format "- %s = %s\n" k v)))
    "Configuration problem\n\nProblematic configurations:\n\n"
    bad-configs)
   "\n" msg))

(defn validate-topic-configs! [context topics]
  (let [configured-topics (set (keys (:topic-configs context)))
        unknown-topics (s/difference configured-topics topics)]
    (when-not (empty? unknown-topics)
      (let [formatted-topics (format-topics unknown-topics)
            ks (original-configs context :topic unknown-topics)
            msg (pretty-error-msg
                 (format "Topic configuration is supplied for topic(s) %s, but no generators are specified for them. Stopping because these topic configurations don't do anything. Either add generators for these topics or remove these topic configurations." formatted-topics)
                 ks)]
        (throw (IllegalArgumentException. msg))))))

(defn validate-attr-configs! [context topics]
  (let [configured-topics (set (keys (:attr-configs context)))
        unknown-topics (s/difference configured-topics topics)]
    (when-not (empty? unknown-topics)
      (let [formatted-topics (format-topics unknown-topics)
            ks (original-configs context :attr unknown-topics)
            msg (pretty-error-msg
                 (format "Attribute configuration is supplied for topic(s) %s, but no generators are specified for them. Stopping because these attribute configurations don't do anything. Either add generators for these topics or remove these attribute configurations." formatted-topics)
                 ks)]
        (throw (IllegalArgumentException. msg))))))

(defn validate-attr-shape! [context]
  (doseq [[topic attrs] (get-in context [:configs-by-topic :attr])]
    (doseq [attr attrs]
      (let [gen (get-in context [:generators topic (:ns attr)])
            ns* (name (:ns attr))]
        (if (:solo gen)
          (when (not= (:kind attr) :attribute-primitive)
            (let [msg (pretty-error-msg
                       (format "Complex attribute configuration is supplied for topic %s, but its generated %s is a primitive type. Stopping because these configurations are incompatible. Either change its %s to a complex type or change this configuration to a primitive type." topic ns* ns*)
                       [[(:original-key attr) (:original-value attr)]])]
              (throw (IllegalArgumentException. msg))))
          (when (not= (:kind attr) :attribute-complex)
            (let [msg (pretty-error-msg
                       (format "Primitive attribute configuration is supplied for topic %s, but its generated %s is a complex type. Stopping because these configurations are incompatible. Either change its %s to a primitive type or change this configuration to a complex type." topic ns* ns*)
                       [[(:original-key attr) (:original-value attr)]])]
              (throw (IllegalArgumentException. msg)))))))))

(defn validate-unused-attrs! [context]
  (doseq [[topic attrs] (get-in context [:configs-by-topic :attr])]
    (doseq [attr attrs]
      (when (= (:kind attr) :attribute-complex)
        (when-not (get-in context [:generators topic (:ns attr) :attrs (:attr attr)])
          (let [msg (pretty-error-msg
                     (format "Complex attribute configuration is supplied for topic %s, but there is no generator that creates this attribute. Stopping because this configuration does nothing. Either add a generator for this attribute or remove this configuration." topic)
                     [[(:original-key attr) (:original-value attr)]])]
            (throw (IllegalArgumentException. msg))))))))

(defn validate-shape-ns-conflicts! [context topic ns*]
  (when (and (get-in context [:generators topic ns* :solo])
             (get-in context [:generators topic ns* :attrs]))
    (let [ns-str (name ns*)
          ks (original-gen-configs context topic ns*)
          msg (pretty-error-msg
               (format "Both primitive and complex generator %s configurations were supplied for topic %s. Stopping because these configurations are incompatible. Either use a single %s primitive generator configuration or use one or more complex configurations." ns-str topic ns-str)
               ks)]
      (throw (IllegalArgumentException. msg)))))

(defn validate-shape-conflicts! [context topics]
  (doseq [topic topics]
    (validate-shape-ns-conflicts! context topic :key)
    (validate-shape-ns-conflicts! context topic :value)))

(defn validate-dependency-generator! [context topics topic gen]
  (when (= (:strategy gen) :dependent)
    (let [dep-topic (:topic gen)
          ns-str (name (:ns gen))]
      (when-not (contains? topics dep-topic)
        (let [msg (pretty-error-msg
                   (format "Found a generator for topic %s that is dependent on topic %s, but no generator is defined for %s. Stopping because no data can ever be produced for topic %s. Either define a generator for topic %s or remove this generator." topic dep-topic dep-topic topic dep-topic)
                   [[(:original-key gen) (:original-value gen)]])]
          (throw (IllegalArgumentException. msg))))

      (if-let [attr (:attr gen)]
        (when-not (get-in context [:generators dep-topic (:ns gen) :attrs attr])
          (let [formatted-attr (format-attr (:attr gen))
                msg (pretty-error-msg
                     (format "Found a generator for topic %s that is dependent on attribute %s in topic %s's %s, but no generator is defined for that attribute. Stopping because this generator would always return null. Either define a generator for the attribute or remove this generator." topic formatted-attr dep-topic ns-str)
                     [[(:original-key gen) (:original-value gen)]])]
            (throw (IllegalArgumentException. msg))))
        (when-not (get-in context [:generators dep-topic (:ns gen) :solo])
          (let [msg (pretty-error-msg
                     (format "Found a generator for topic %s that is dependent on topic %s's %s, but there's no primitive generator defined for the %s. Stopping because this generator is incompatible. Either define a generator for the %s or remove this dependency." topic dep-topic ns-str ns-str ns-str)
                     [[(:original-key gen) (:original-value gen)]])]
            (throw (IllegalArgumentException. msg))))))))

(defn validate-ns-dependencies! [context topics topic ns*]
  (let [generator (get-in context [:generators topic ns*])]
    (if (:solo generator)
      (doseq [gen (:solo generator)]
        (validate-dependency-generator! context topics topic gen))
      (doseq [[attr generators] (:attrs generator)]
        (doseq [gen generators]
          (validate-dependency-generator! context topics topic gen))))))

(defn validate-dependencies! [context topics]
  (doseq [topic topics]
    (when (not (empty? (get-in context [:generators topic :dependencies])))
      (validate-ns-dependencies! context topics topic :key)
      (validate-ns-dependencies! context topics topic :value))))

(defn validate-configuration! [context]
  (let [topics (set (keys (:generators context)))]
    (validate-topic-configs! context topics)
    (validate-attr-configs! context topics)
    (validate-attr-shape! context)
    (validate-unused-attrs! context)
    (validate-shape-conflicts! context topics)
    (validate-dependencies! context topics)

    (dissoc context :configs-by-topic)))
