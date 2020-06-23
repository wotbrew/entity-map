(ns com.wotbrew.entity-map-test
  (:refer-clojure :exclude [replace])
  (:require [clojure.test :refer :all]
            [com.wotbrew.entity-map :refer :all]))

(deftest readme-example-test
  (let [data
        [{:id 1, :name "foo", :age 10},
         {:id 2, :name "bar", :age 42},
         {:id 3, :name "baz", :age 10}]

        em (wrap-coll :id data)

        _ (is (= {1 {:id 1, :name "foo", :age 10},
                  2 {:id 2, :name "bar", :age 42},
                  3 {:id 3, :name "baz", :age 10}} em))

        _ (is (= #{1, 3} (eq em :age 10)))

        em2 (assoc em 4 {:name "qux", :age 10})

        _ (is (= #{1, 3, 4} (eq em2 :age 10)))]))

(deftest map-behaviour-test

  (is (= {} (entity-map)))
  (is (= {} (wrap {})))
  (is (= {} (wrap nil)))
  
  (is (= {0 {:name "foo"}} (entity-map 0 {:name "foo"})))
  (is (= {0 {:name "foo"}, 1 {:name "bar"}} (entity-map 0 {:name "foo"} 1 {:name "bar"})))


  (is (= {0 {:name "foo"}} (wrap {0 {:name "foo"}}))))

;; TODO generative tests
