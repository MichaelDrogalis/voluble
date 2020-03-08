(ns io.mdrogalis.manifest.build-manifest
  (:require [cheshire.core :as json]))

(defn build-manifest []
  (let [version (System/getProperty "voluble.version")]
    {:title "Voluble"
     :version version
     :name "voluble"
     :description "Intelligent data generator for Apache Kafka. Generates streams of realistic data with support for cross-topic relationships, tombstoning, configurable rates, and more."
     :tags ["generator" "datagen" "demo" "simulation"]
     :owner {:name "Michael Drogalis"
             :username "mdrogalis"
             :type "user"
             :url "https://twitter.com/MichaelDrogalis"}
     :license [{:name "Eclipse Public License"
                :url "https://www.eclipse.org/legal/epl-2.0/"}]
     :features {:supported_encodings ["any"]}
     :documentation_url "https://github.com/MichaelDrogalis/voluble"
     :component_types ["source"]
     :support {}
     :requirements []}))

(defn spit-manifest [path]
  (spit path (json/generate-string (build-manifest) {:pretty true})))

(defn -main [path & args]
  (spit-manifest path))
