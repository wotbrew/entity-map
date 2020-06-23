# entity-map

A low ceremony, efficient, auto indexing data structure.

**not quite ready yet**

## Features

- It behaves as a map of {id attributes} where attributes is another map, `get`, `assoc`, `update` and so on all work as expected.
- Very cheap to create, no schema, no setup.
- Persistent data structure, cheap to modify.
- Indexes are created in response to queries, and then cached transparently. (You can still force indexes ahead of time if you want)
- Indexes once cached are maintained incrementally as modified versions of the entity map are created.
- Different index types, value, unique and btree indexes for range queries.

## Rationale

Often in clojure code you create indexes against your data, often with functions like `group-by`, and you have to co-ordinate 
creating these indexes, and making them available to consuming logic.

As your data set changes, you would have to manually invalidate any indexes with new values.

So we would like a low-cost data structure, providing automatic indexes that competes with manual `group-by`, `index-by` style solutions.

## Usage

Given some data 

```clojure
(def data 
 [{:id 1, :name "foo", :age 10},
  {:id 2, :name "bar", :age 42},
  {:id 3, :name "baz", :age 10}])
```

Require the entity-map namespace

```clojure 
(require '[com.wotbrew.entity-map :as em])
```

Create an entity map using `wrap-coll` as we are starting with a collection. We provide a key function, in this case the `:id` attribute.

```clojure
(def em (em/wrap-coll :id data)) 
``` 

If we print the entity map, we will see it as a map of {id attributes}. Not very interesting.

```clojure
{1 {:id 1, :name "foo", :age 10},
 2 {:id 2, :name "bar", :age 42},
 3 {:id 3, :name "baz", :age 10}}
```

Lets write a query to find the ids of entities with the age `10`

```clojure
(em/eq em :age 10)
```

You will receive a set of ids where :age is 10.

```clojure
#{1, 3}
```

When you perform this query - an index is computed for :age behind the scenes and is cached
against the entity map, much like how clojure caches hash codes for persistent datastructures.

Now lets add another entity.

```clojure 
(def em2 (assoc em 4 {:name "qux", :age 10}))
```

And ask again, for entities where :age is `10`

```clojure
(em/eq em2 :age 10)
;; returns
#{1, 3, 4}
```

Notice the new entity makes its way magically into our index. The index was not actually
recomputed, indexes are maintained incrementally for modified versions of entity maps.

Well done. Now explore the rest of the API, some useful functions:

`ascending` and `descending` lets you perform range queries on sortable attributes.

`uniq` is like `eq` but maintains a one-to-one index, that is you can use it for unique attributes and return a single id rather than a set.

`get-eq`, `get-uniq`, `get-ascending` and `get-descending` return you the values in the map rather than the ids for your query.

`force-index` lets you ensure indexes exist, if it matters when you pay the cost.

`wrap` wraps an existing map of `{id attributes}`, returning an entity map. Very cheap!

Optional modification functions `add`, `delete` and `edit` provide some optimisations over `assoc`, `dissoc` and `update`.

## Comparisons

### Datascript

Datascript provides a full eav database with a datalog query engine. The query engine 
is quite sophisticated. It requires you to install a schema for indexes. It is expensive 
to create (and query using datalog at least) datascript databases compared to entity-map.

Datalog allows for logical queries including joins entity-map does not.

## Future plans

I want to limit the scope of this library so it doesn't become another database, that said:

- Basic joins 
- ClojureScript support
- Composite indexes
- Indexing options for sub maps
- Indexing options for sub collections

## Contributing

Pull requests are welcome, see issues for ideas for improvement.

## License

Copyright © 2020 Daniel Stone

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
