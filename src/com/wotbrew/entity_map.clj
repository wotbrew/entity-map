(ns com.wotbrew.entity-map
  "A collection of maps keyed with an id. Indexes are provided so you can lookup ids of maps
  meeting matching query.

  Indexes can be computed ahead of time, or are created as needed in order to answer queries.

  Indexes are cached against the EntityMap and are incrementally maintained from then on.

  Provides support for value, unique and sorted (btree) indices.

  Entity maps support all regular map functions, but do not allow non-map values."
  (:require [clojure.set :as set])
  (:refer-clojure :exclude [replace])
  (:import (clojure.lang Counted ILookup Seqable IFn Associative IPersistentCollection IMeta IObj IReduceInit IKVReduce IPersistentMap IHashEq MapEquivalence)
           (java.util Map)))

(set! *warn-on-reflection* true)

(defprotocol EntityMapLazyIndex
  "Do not extend, implementation detail only."
  (-eq [this])
  (-eq-idx [this k])
  (-uniq [this])
  (-uniq-idx [this k])
  (-btree [this])
  (-btree-idx [this k]))

(defn- create-eq [primary k]
  (persistent!
    (reduce-kv
      (fn [idx id m]
        (let [v (get m k ::not-found)]
          (if (identical? ::not-found v)
            idx
            (let [ids (get idx v #{})]
              (assoc! idx v (conj ids id))))))
      (transient {})
      primary)))

(defn- create-uniq [primary k]
  (persistent!
    (reduce-kv
      (fn [idx id m]
        (let [v (get m k ::not-found)]
          (if (identical? ::not-found v)
            idx
            (assoc! idx v id))))
      (transient {})
      primary)))

(defn- create-btree [primary k]
  (reduce-kv
    (fn [idx id m]
      (let [v (get m k ::not-found)]
        (if (identical? ::not-found v)
          idx
          (let [ids (get idx v #{})]
            (assoc idx v (conj ids id))))))
    (sorted-map)
    primary))

(def ^:private empty-entity-map)
(declare replace delete)

(deftype EntityMap
  [primary
   meta

   ^:unsynchronized-muEntityMap eq
   ^:unsynchronized-muEntityMap uniq
   ^:unsynchronized-muEntityMap btree]

  Object
  (hashCode [this] (.hashCode primary))
  (equals [this o2] (.equals primary o2))

  IHashEq
  (hasheq [this]
    (.hasheq ^IHashEq primary))

  IMeta
  (meta [this] meta)
  IObj
  (withMeta [this ipm] (EntityMap. primary ipm eq uniq btree))

  Counted
  (count [this] (count primary))

  ILookup
  (valAt [this id] (get primary id))
  (valAt [this id not-found] (get primary id not-found))

  IFn
  (invoke [this id] (get primary id))
  (invoke [this id not-found] (get primary id not-found))
  (applyTo [this args] (apply get primary args))

  Seqable
  (seq [this] (seq primary))

  IKVReduce
  (kvreduce [this f init]
    (reduce-kv f init primary))

  IReduceInit
  (reduce [this f o]
    (reduce f o primary))

  Associative
  (containsKey [this o]
    (.containsKey ^Associative primary o))
  (entryAt [this o]
    (.entryAt ^Associative primary o))
  (assoc [this o o1] (replace this o o1))

  Map
  (size [this] (count this))
  (isEmpty [this] (= 0 (count this)))
  (containsValue [this value] (.containsValue ^Map primary value))
  (get [this key] (.get ^Map primary key))
  (keySet [this] (.keySet ^Map primary))
  (values [this] (.values ^Map primary))
  (entrySet [this] (.entrySet ^Map primary))

  MapEquivalence
  IPersistentMap
  (assocEx [this o o1]
    (if (contains? primary o)
      (throw (Exception. "Key already present"))
      (assoc this o o1)))
  (without [this o] (delete this o))

  IPersistentCollection
  (cons [this o]
    (if (map? o)
      (reduce-kv replace this o)
      (let [[id m] o]
        (replace this id m))))
  (empty [this] empty-entity-map)
  (equiv [this o]
    (and (instance? IPersistentCollection o)
         (.equiv ^IPersistentCollection o primary)))

  Iterable
  (iterator [this]
    (.iterator ^Iterable primary))

  EntityMapLazyIndex
  (-eq-idx [this k]
    (if (some? eq)
      (if-some [idx (get eq k)]
        idx
        (let [idx (create-eq primary k)]
          (set! (.-eq this) (assoc eq k idx))
          idx))
      (let [eq {k (create-eq primary k)}]
        (set! (.-eq this) eq)
        (get eq k))))
  (-eq [this] eq)
  (-uniq-idx [this k]
    (if (some? uniq)
      (if-some [idx (get uniq k)]
        idx
        (let [idx (create-uniq primary k)]
          (set! (.-uniq this) (assoc uniq k idx))
          idx))
      (let [uniq {k (create-uniq primary k)}]
        (set! (.-uniq this) eq)
        (get uniq k))))
  (-uniq [this] uniq)
  (-btree-idx [this k]
    (if (some? btree)
      (if-some [idx (get btree k)]
        idx
        (let [idx (create-btree primary k)]
          (set! (.-btree this) (assoc btree k idx))
          idx))
      (let [btree {k (create-btree primary k)}]
        (set! (.-btree this) eq)
        (get btree k))))
  (-btree [this] btree))

(def empty-entity-map (->EntityMap {} nil nil nil nil))

(defn entity-map
  "Creates and returns an empty EntityMap
  given the id > map pairs."
  ([] empty-entity-map)
  ([id m] (replace empty-entity-map id m))
  ([id m & more]
   (loop [em (replace empty-entity-map id m)
          idms more]
     (if-some [[id m & idms] (seq idms)]
       (recur (replace em id m) idms)
       em))))

(defn wrap
  "Given a map of id -> m returns a new EntityMap. Extremely cheap."
  [m]
  (if (empty? m)
    empty-entity-map
    (->EntityMap m nil nil nil nil)))

(defn wrap-coll
  "Creates an EntityMap from the collection using (f element) to determine 
  the id of each element."
  [f coll]
  (-> (transient {})
      (as-> m (reduce (fn [m x] (assoc! m (f x) x)) m coll))
      (persistent!)))

(defmethod print-method EntityMap
  [em writer]
  (print-method (.-primary ^EntityMap em) writer))

(defn- no-indices?
  "Allows for fast-paths when the EntityMap is new."
  [em]
  (let [^EntityMap em em]
    (and (nil? (-eq em))
         (nil? (-uniq em))
         (nil? (-btree em)))))

(defn- add-single-key
  [em id k v]
  (let [^EntityMap em em
        primary (.-primary em)
        old-map (primary id)
        old-val (get old-map k ::not-found)]
    (cond
      (identical? old-val v) em
      (identical? ::not-found old-val)
      (let [new-map (assoc old-map k v)
            new-primary (assoc primary id new-map)

            old-eq (-eq em)
            old-btree (-btree em)
            old-uniq (-uniq em)

            eq (when (some? old-eq)
                 (if-some [old-idx (get old-eq k)]
                   (let [old-ids (get old-idx v #{})

                         ids (conj old-ids id)
                         idx (assoc old-idx v ids)]
                     (assoc old-eq k idx))
                   old-eq))

            uniq (when (some? old-uniq)
                   (if-some [old-idx (get old-uniq k)]
                     (let [idx (assoc old-idx v id)]
                       (assoc old-uniq k idx))
                     old-uniq))

            btree (when (some? old-btree)
                    (if-some [old-idx (get old-btree k)]
                      (let [old-ids (get old-idx #{})

                            ids (conj old-ids old-ids)
                            idx (assoc old-idx v ids)]
                        (assoc old-btree k idx))
                      old-btree))]

        (->EntityMap new-primary nil eq uniq btree))


      :else
      (let [new-map (assoc old-map k v)
            new-primary (assoc primary id new-map)

            old-eq (-eq em)
            old-btree (-btree em)
            old-uniq (-uniq em)

            eq (when (some? old-eq)
                 (if-some [old-idx (get old-eq k)]
                   (let [old-idx (update old-idx old-val disj id)

                         old-ids (get old-idx v #{})

                         ids (conj old-ids id)
                         idx (assoc old-idx v ids)]
                     (assoc old-eq k idx)))
                 old-eq)

            uniq (when (some? old-uniq)
                   (if-some [old-idx (get old-uniq k)]
                     (let [old-idx (dissoc old-idx old-val)
                           idx (assoc old-idx v id)]
                       (assoc old-uniq k idx))
                     old-uniq))

            btree (when (some? old-btree)
                    (if-some [old-idx (get old-btree k)]
                      (let [old-idx (update old-idx old-val disj id)
                            old-ids (get old-idx #{})

                            ids (conj old-ids old-ids)
                            idx (assoc old-idx v ids)]
                        (assoc old-btree k idx))
                      old-btree))]

        (->EntityMap new-primary nil eq uniq btree)))))

(defn- delete-single-key
  [em id k]
  (let [^EntityMap em em
        primary (.-primary em)
        old-map (primary id)
        old-val (get old-map k ::not-found)]
    (cond
      (identical? ::not-found old-val) em
      :else
      (let [new-map (dissoc old-map k)
            new-primary (assoc primary id new-map)

            old-eq (-eq em)
            old-btree (-btree em)
            old-uniq (-uniq em)

            eq (when (some? old-eq)
                 (if-some [old-idx (get old-eq k)]
                   (let [idx (update old-idx old-val disj id)]
                     (assoc old-eq k idx))
                   old-eq))

            uniq (when (some? old-uniq)
                   (if-some [old-idx (get old-uniq k)]
                     (let [idx (dissoc old-idx old-val)]
                       (assoc old-uniq k idx))
                     old-uniq))

            btree (when (some? old-btree)
                    (if-some [old-idx (get old-btree k)]
                      (let [idx (update old-idx old-val disj id)]
                        (assoc old-btree k idx))
                      old-btree))]

        (->EntityMap new-primary nil eq uniq btree)))))

(defn add
  "Adds the contents of m to the map given by the id. If you want to entirely replace the map, use replace."
  [em id m]
  (let [^EntityMap em em
        primary (.-primary em)
        old-val (get primary id)]
    (cond
      (no-indices? em) (->EntityMap (assoc primary id (if old-val (conj old-val m) m)) nil nil nil nil)
      (= 0 (count m)) (->EntityMap (assoc primary id (if old-val old-val {})) nil (-eq em) (-uniq em) (-btree em))
      :else (reduce-kv (fn [em k v] (add-single-key em id k v)) em m))))

(defn delete
  "Deletes the map given its id, returning the resulting EntityMap."
  [em id]
  (let [^EntityMap em em
        primary (.-primary em)]
    (if (contains? primary id)
      (if (no-indices? em)
        (->EntityMap (dissoc primary id) nil nil nil nil)
        (let [em (reduce-kv (fn [em k _] (delete-single-key em id k)) em (get primary id))]
          (->EntityMap (dissoc primary id) nil (-eq em) (-uniq em) (-btree em))))
      em)))

(defn replace
  "Overwrites the current value of the map at id with the new map.
  Returns the new EntityMap."
  [em id m]
  (if (no-indices? em)
    (let [primary (.-primary ^EntityMap em)
          old-val (get primary id)
          _ (assert (map? m) "new value must be a map.")]
      (if (identical? m old-val)
        em
        (->EntityMap (assoc primary id m) nil nil nil nil)))
    (let [^EntityMap em em
          primary (.-primary em)
          old-val (get primary id)
          _ (assert (map? m) "new value must be a map.")]

      (if (identical? m old-val)
        em
        (let [new-keys (set (keys m))
              old-keys (set (keys old-val))

              del-keys (set/difference old-keys new-keys)

              em (reduce (fn [em k] (delete-single-key em id k)) em del-keys)
              em (reduce-kv (fn [em k v] (add-single-key em id k v)) em m)]

          em)))))

(defn edit
  "Applies a function to the map given by the id, and returns the resulting EntityMap containing the new map."
  [em id f & args]
  (cond
    (no-indices? em)
    (let [primary (.-primary ^EntityMap em)
          old-val (get primary id)
          new-val (apply f old-val args)
          _ (assert (map? new-val) "new value must be a map.")]
      (if (identical? new-val old-val)
        em
        (->EntityMap (assoc primary id new-val) nil nil nil nil)))

    (identical? f assoc)
    (loop [kvs args
           em em]
      (if-some [[k v & kvs] (seq kvs)]
        (recur kvs (add-single-key em id k v))
        em))

    (identical? f dissoc) (reduce (fn [em k] (delete-single-key em id k)) em args)
    (identical? f merge) (reduce (fn [em m] (add em id m)) em args)
    (identical? f into) (reduce (fn [em m] (add em id m)) em args)
    :else
    (let [^EntityMap em em
          primary (.-primary em)
          old-val (get primary id)
          new-val (apply f old-val args)
          _ (assert (map? new-val) "new value must be a map.")]

      (if (identical? new-val old-val)
        em
        (let [new-keys (set (keys new-val))
              old-keys (set (keys old-val))

              del-keys (set/difference old-keys new-keys)

              em (reduce (fn [em k] (delete-single-key em id k)) em del-keys)
              em (reduce-kv (fn [em k v] (add-single-key em id k v)) em new-val)]

          em)))))

(defn eq
  "Returns the set of ids where k = v"
  ([em k v]
   (let [idx (-eq-idx em k)]
     (get idx v #{})))
  ([em k v & more]
   (loop [ret (eq em k v)
          kvs more]
     (if (= #{} ret)
       ret
       (if-some [[k v & kvs] (seq kvs)]
         (recur (set/intersection ret (eq em k v)) kvs)
         ret)))))

(defn get-eq
  "Like `eq` but returns a sequence of matching attribute maps rather than a set of ids."
  ([em k v] (map em (eq em k v)))
  ([em k v & more] (map em (apply eq em k v more))))

(defn uniq
  "Returns the id where k = v, assumes v is mapped to one id."
  [em k v]
  (let [idx (-uniq-idx em k)]
    (get idx v)))

(defn get-uniq
  "Like `uniq` but returns the attribute map rather than the id."
  [em k v]
  (em (uniq em k v)))

(defn ascending
  "Returns a ascending sequence of ids where k test v e.g

  (ascending em :count <= 3) find all ids where :count <= 3 from low to high."
  ([em k test v]
   (let [idx (-btree-idx em k)]
     (subseq idx test v)))
  ([em k test v end-test v2]
   (let [idx (-btree-idx em k)]
     (subseq idx test v end-test v2))))

(defn get-ascending
  "Like `ascending` but returns a sequence of the matching attribute maps rather than ids."
  ([em k test v] (map em (ascending em k test v)))
  ([em k test v end-test v2] (map em (ascending em k test v end-test v2))))

(defn descending
  "Returns a descending sequence of ids where k test v e.g

  (descending em :count <= 3) find all ids where :count <= 3 from high to low."
  ([em k test v]
   (let [idx (-btree-idx em k)]
     (rsubseq idx test v)))
  ([em k test v end-test v2]
   (let [idx (-btree-idx em k)]
     (rsubseq idx test v end-test v2))))

(defn get-descending
  "Like `descending` but returns a sequence of the matching attribute maps rather than ids."
  ([em k test v] (map em (descending em k test v)))
  ([em k test v end-test v2] (map em (descending em k test v end-test v2))))

(defn force-index
  "Ensures an index exists for the key.

  Indexes:

  :eq
  :uniq
  :btree.

  Can supply multiple pairs, and the same key multiple times e.g

  (force-index em :foo :eq, :bar :uniq, :baz :uniq, :baz :btree)"
  ([em k index]
   (case index
     :eq (-eq-idx em k)
     :uniq (-uniq-idx em k)
     :btree (-btree-idx em k)
     (throw (Exception. "Unknown index")))
   em)
  ([em k index & more]
   (loop [em (force-index em k index)
          pairs more]
     (if-some [[k index & pairs] (seq pairs)]
       (recur (force-index em k index) pairs)
       em))))

(declare query)

(defn- simple-query
  [em term]
  (case (count term)
    2 (let [[k v] term] (eq em k v))
    3 (case (first term)
        :eq (apply eq em (rest term))
        :uniq (if-some [id (apply uniq em (rest term))] #{id} #{})
        :range (set (apply ascending em (rest term)))


        :or (apply set/union (map #(query em %) (rest term)))
        :and (apply query em (rest term)))))

(defn query
  "Does an intersection query given the data terms.

  :eq [k v] or [:eq k v]
  :uniq [:uniq k v]
  :range [:range k test v] or [:range k test v end-test v2]

  :or [:or term1, term2, ...]
  :and [:and term1, term2 ...]

  No join support yet.

  Returns a set of ids meeting the conjunction of the terms."
  [em term & terms]
  (loop [ret (simple-query em term)
         terms terms]
    (if (= ret #{})
      ret
      (if-some [[term & terms] (seq terms)]
        (recur (set/intersection ret (simple-query em term)) terms)
        ret))))