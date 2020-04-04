(ns io.mdrogalis.voluble.validate
  (:require [clojure.set :as s]
            [clojure.string :refer [join]]))

(defn format-topics [topics]
  (join ", " topics))

(defn format-configs [context k topics]
  (let [ks (map (fn [t] (get-in context [:raw-configs k t])) topics)]
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

(defn validate-configuration! [context]
  (let [topics (set (keys (:generators context)))]
    (validate-topic-configs! context topics)
    (validate-attr-configs! context topics)
    
    (dissoc context :raw-configs)))
