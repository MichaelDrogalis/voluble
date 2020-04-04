(ns io.mdrogalis.voluble.validate
  (:require [clojure.set :as s]
            [clojure.string :refer [join]]))

(defn format-topics [topics]
  (join ", " topics))

(defn format-configs [context k topics]
  (let [ks (mapcat (fn [t] (map :original-key (get-in context [:configs-by-topic k t]))) topics)]
    (join ", " ks)))

(defn validate-topic-configs! [context topics]
  (let [configured-topics (set (keys (:topic-configs context)))
        unknown-topics (s/difference configured-topics topics)]
    (when-not (empty? unknown-topics)
      (let [formatted-topics (format-topics unknown-topics)
            formatted-configs (format-configs context :topic unknown-topics)
            msg (format "Topic configuration is supplied for topic(s) %s, but no generators are specified for them. Stopping because these topic configurations don't do anything. Either add generators for these topics or remove these topic configurations. Problematic configurations are: %s" formatted-topics formatted-configs)]
        (throw (IllegalArgumentException. msg))))))

(defn validate-attr-configs! [context topics]
  (let [configured-topics (set (keys (:attr-configs context)))
        unknown-topics (s/difference configured-topics topics)]
    (when-not (empty? unknown-topics)
      (let [formatted-topics (format-topics unknown-topics)
            formatted-configs (format-configs context :attr unknown-topics)
            msg (format "Attribute configuration is supplied for topic(s) %s, but no generators are specified for them. Stopping because these attribute configurations don't do anything. Either add generators for these topics or remove these attribute configurations. Problematic configurations are: %s" formatted-topics formatted-configs)]
        (throw (IllegalArgumentException. msg))))))

(defn validate-attr-shape! [context]
  (doseq [[topic attrs] (get-in context [:configs-by-topic :attr])]
    (doseq [attr attrs]
      (let [gen (get-in context [:generators topic (:ns attr)])
            ns* (name (:ns attr))]
        (if (:solo gen)
          (when (not= (:kind attr) :attribute-primitive)
            (let [msg (format "Primitive attribute configuration is supplied for topic %s, but its generated %s is a complex type. Stopping because these configurations are incompatible. Either change its %s to a primitive type or change this configuration to a complex type. Problematic configuration is: %s" topic ns* ns* (:original-key attr))]
              (throw (IllegalArgumentException. msg))))
          (when (not= (:kind attr) :attribute-complex)
            (let [msg (format "Complex attribute configuration is supplied for topic %s, but its generated %s is a primitive type. Stopping because these configurations are incompatible. Either change its %s to a complex type or change this configuration to a primitive type. Problematic configuration is: %s" topic ns* ns* (:original-key attr))]
              (throw (IllegalArgumentException. msg)))))))))

(defn validate-unused-attrs! [context]
  (doseq [[topic attrs] (get-in context [:configs-by-topic :attr])]
    (doseq [attr attrs]
      (when (= (:kind attr) :attribute-complex)
        (when-not (get-in context [:generators topic (:ns attr) :attrs (:attr attr)])
          (let [msg (format "Complex attribute configuration is supplied for topic %s, but there is no generator that creates this attribute. Stopping because this configuration does nothing. Either add a generator for this attribute or remove this configuration. Problematic configuration is: %s" topic (:original-key attr))]
            (throw (IllegalArgumentException. msg))))))))

(defn validate-configuration! [context]
  (let [topics (set (keys (:generators context)))]
    (validate-topic-configs! context topics)
    (validate-attr-configs! context topics)
    (validate-attr-shape! context)
    (validate-unused-attrs! context)

    (dissoc context :configs-by-topic)))
