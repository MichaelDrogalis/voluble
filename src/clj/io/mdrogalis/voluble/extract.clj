(ns io.mdrogalis.voluble.extract
  (:require [io.mdrogalis.voluble.parse :as p]))

(defn update-dependencies [topics topic parsed-v]
  (if (= (:strategy parsed-v) :dependent)
    (let [dep (:topic parsed-v)]
      (update-in topics [topic :dependencies] (fnil conj #{}) dep))
    topics))

(defn store-generator [topics {:keys [topic attr] :as parsed-k} parsed-v]
  (if attr
    (update-in topics [topic (:ns parsed-k) :attrs attr] (fnil conj []) parsed-v)
    (update-in topics [topic (:ns parsed-k) :solo] (fnil conj []) parsed-v)))

(defn extract-generators [kvs]
  (reduce-kv
   (fn [all {:keys [topic] :as k} v]
     (if (:generator k)
       (let [parsed-val (p/parse-value k v)
             augmented-val (p/augment-parsed-val k parsed-val)]
         (-> all
             (store-generator k augmented-val)
             (update-dependencies topic augmented-val)))
       all))
   {}
   kvs))

(defn extract-global-configs [kvs]
  (reduce-kv
   (fn [all k v]
     (if (= (:kind k) :global)
       (assoc-in all (:config k) v)
       all))
   {}
   kvs))

(defn extract-topic-configs [kvs]
  (reduce-kv
   (fn [all k v]
     (if (= (:kind k) :topic)
       (assoc-in all (into [(:topic k)] (:config k)) v)
       all))
   {}
   kvs))

(defn extract-attr-configs [kvs]
  (reduce-kv
   (fn [all k v]
     (if (some #{(:kind k)} #{:attribute-primitive :attribute-complex})
       (assoc-in all (into [(:topic k) (:ns k) (:attr k)] (:config k)) v)
       all))
   {}
   kvs))
