(ns com.wotbrew.entity-map-test
  (:refer-clojure :exclude [replace])
  (:require [clojure.test :refer :all]
            [com.wotbrew.entity-map :refer :all]))

(deftest map-behaviour-test

  (is (= {} (entity-map)))
  (is (= {} (wrap {})))
  (is (= {} (wrap nil)))
  
  (is (= {0 {:name "foo"}} (entity-map 0 {:name "foo"})))
  (is (= {0 {:name "foo"}, 1 {:name "bar"}} (entity-map 0 {:name "foo"} 1 {:name "bar"})))


  (is (= {0 {:name "foo"}} (wrap {0 {:name "foo"}}))))

;; TODO generative tests
