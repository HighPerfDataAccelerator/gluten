# Experimental Q5 second-hop Bloom filter (Velox)

`spark.gluten.experimental.q5SecondHopBloom.enabled` is a dynamic boolean,
default `false`. It enables a narrow Q5-shaped supplier-to-lineitem Bloom
filter after the existing runtime cascade has filtered supplier by nation.
The original two-key inner join still checks every surviving row.

Register the extension **after** Gluten's extension and keep the existing
runtime cascade enabled:

```properties
spark.sql.extensions=org.apache.gluten.extension.GlutenSessionExtensions,org.apache.gluten.experimental.q5.Q5SecondHopBloomExtension
spark.gluten.sql.columnar.backend.velox.runtimeBloom.cascade.enabled=true
spark.gluten.experimental.q5SecondHopBloom.enabled=true
```

Preserve any other extensions already configured. The rule uses the existing
cascade `maxFilters` and `maxCreationKeyBytes` limits and Spark's runtime-Bloom
application-side scan threshold. It adds at most one filter, requires exactly
one matching join, and leaves unsupported plans unchanged. Inspect the optimized
plan for `gluten_q5_second_hop_bloom_filter` and the driver log for
`Q5_SECOND_HOP_BLOOM added=1`; enabling the flag alone does not prove activation.

The tested setup was Spark 4.0.1 / Scala 2.13 / Java 17. This is an opt-in
experiment, not a general join policy: the extra supplier scan and Bloom creation
helped the tested clustered SF1K Q5 but regressed its SF100 counterpart. Keep
other queries and all baseline settings unchanged when comparing the flag off/on.
