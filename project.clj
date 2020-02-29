(defproject io.mdrogalis/voluble "0.1.0-SNAPSHOT"
  :description "Intelligent data generator for Apache Kafka. Generates streams of realistic data with support for cross-topic relationships, tombstoning, configurable rates, and more."
  :url "http://github.com/MichaelDrogalis/voluble"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [com.github.javafaker/javafaker "1.0.2"]]
  :jvm-opts ["-Xverify:none"]
  :profiles
  {:provided
   {:dependencies [[org.apache.kafka/connect-api "2.4.0"]]}
   :dev
   {:global-vars {*warn-on-reflection* true}
    :dependencies [[org.clojure/test.check "1.0.0"]
                   [com.gfredericks/test.chuck "0.2.10"]
                   [com.theinternate/generators.graph "0.0-37"]
                   [criterium "0.4.5"]]}
   :uberjar
   {:aot :all
    :uberjar-name "voluble-uberjar-%s.jar"}})
