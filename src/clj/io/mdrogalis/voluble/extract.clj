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
       (let [parsed-val (p/parse-generator-value k v)
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
       (let [parsed-val (p/parse-global-value k v)]
         (assoc-in all (:config k) parsed-val))
       all))
   {}
   kvs))

(defn extract-topic-configs [context kvs]
  (reduce-kv
   (fn [ctx k v]
     (if (= (:kind k) :topic)
       (let [parsed-val (p/parse-topic-value k v)
             retained-config (select-keys k [:original-key])]
         (-> ctx
             (assoc-in (into [:topic-configs (:topic k)] (:config k)) parsed-val)
             (update-in [:configs-by-topic :topic (:topic k)] (fnil conj []) retained-config)))
       ctx))
   context
   kvs))

(defn extract-attr-configs [context kvs]
  (reduce-kv
   (fn [ctx k v]
     (if (some #{(:kind k)} #{:attribute-primitive :attribute-complex})
       (let [parsed-val (p/parse-attr-value k v)
             retained-config (select-keys k [:original-key :kind :ns])]
         (-> ctx
             (assoc-in (concat [:attr-configs (:topic k) (:ns k)] (:attr k) (:config k)) parsed-val)
             (update-in [:configs-by-topic :attr (:topic k)] (fnil conj []) retained-config)))
       ctx))
   context
   kvs))
