To perform SQL SELECTs over Aerospike Predicate API, get an instance of AQLSelectEx class by supplying a schema.
Schema is a simple Map<String, Map<String, Integer>>, where top-level keys are namespace[.set] and second-level keys
are bin (or for MAP type bins bin.var), and leaf values are bin (or MAP bin.var) types. 

As an example,

```java
final HashMap FOO_BAR_BAZ = new HashMap() {{
        put("namespace.set0", new HashMap() {{
            put("foo", ParticleType.INTEGER);
            put("bar", ParticleType.DOUBLE);
            put("baz", ParticleType.STRING);
            put("qux", ParticleType.GEOJSON);
            put("quux", ParticleType.LIST);
            put("corge", ParticleType.MAP);
            put("corge.uier", ParticleType.INTEGER);
        }});
        put("namespace.set1", new HashMap() {{
            put("grault", ParticleType.INTEGER);
            put("garply", ParticleType.STRING);
        }});
    }};
AQLSelectEx selectEx = AQLSelectEx.forSchema(FOO_BAR_BAZ);
```

Bins not found in the schema are considered of type String (and you could safely omit String bins from the schema). 

Now you can perform more powerful SELECTs than AQL tool, using complex expression that involve any number of bins in WHERE clause,
just as you do with Predicate Filter API. Every predicate is supported. (But Filter API is supported too, using WITH syntax.)

Just see:

```java
Statement statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace.set WITH (baz='a') WHERE (foo>2 AND (bar <=3 OR foo>5) AND bar >3) OR NOT (qux WITHIN CAST('{\"type\": \"Polygon\", \"coordinates\": [0.0, 0.0],[1.0, 0.0],[1.0, 1.0],[0.0, 1.0],[0.0, 0.0]}' AS GEOJSON)");
```

This SELECT is fully equivalent to

```java
Statement reference = new Statement();
reference.setSetName("set");
reference.setNamespace("namespace");
reference.setBinNames("foo", "bar", "baz");
reference.setFillter(Filter.stringEqual("baz", "a"));
reference.setPredExp(// 20 expressions in RPN
      PredExp.integerBin("foo")
    , PredExp.integerValue(2)
    , PredExp.integerGreater()
    , PredExp.integerBin("bar")
    , PredExp.integerValue(3)
    , PredExp.integerLessEq()
    , PredExp.integerBin("foo")
    , PredExp.integerValue(5)
    , PredExp.integerGreater()
    , PredExp.or(2)
    , PredExp.and(2)
    , PredExp.integerBin("bar")
    , PredExp.integerValue(3)
    , PredExp.integerGreater()
    , PredExp.and(2)
    , PredExp.geoJSONBin("qux")
    , PredExp.geoJSONValue("{\"type\": \"Polygon\", \"coordinates\": [0.0, 0.0],[1.0, 0.0],[1.0, 1.0],[0.0, 1.0],[0.0, 0.0]}")
    , PredExp.geoJSONWithin()
    , PredExp.not()
    , PredExp.or(2)
);
```

So, the main point of this project is code readability and maintainability. It is much easier to write and manage a couple of SQL SELECTs
rather than a nearly non-comprehencible walls of PredExp calls in RPN.

For full syntax (in ANTLR4 notation) please refer to [grammar file](./AQLSelectEx/src/main/antlr4/io/github/pastorgl/aqlselectex/AQLSelectEx.g4).
