# Q8: broadcast the filtered part input before shuffling lineitem

This opt-in experiment changes the physical join hint for the existing filtered
`part` input in the supported TPC-H Q8 plan. The query still uses the same inner
join, input plans, equality condition and outputs. Duplicate keys are retained.

On the measured SF1K input, AQE kept this join partitioned because the filtered
part input exceeded the 8 MiB threshold. Selecting broadcast before physical
planning avoids shuffling the large lineitem input. The extension is independent
of the Q5 experiment.

## Enable

Append this extension after the existing entries in `spark.sql.extensions`,
including `org.apache.gluten.extension.GlutenSessionExtensions`. This matches the
measured runner's extension order:

```text
org.apache.gluten.experimental.q8.Q8EarlyBroadcastExtension
```

After creating the Spark session, enable the setting only for the Q8 experiment:

```sql
SET spark.gluten.experimental.q8EarlyPartBroadcast.enabled=true;
```

The setting defaults to `false`. The extension installs its rule after Spark has
formed ordinary equijoins; it preserves other extra optimizer rules. Check the
optimized plan for one broadcast hint on the filtered part input, and the physical
plan for the part/lineitem broadcast hash join. The driver logs
`Q8_EARLY_PART_BROADCAST matched=1` when it changes the hint. A loaded extension
alone does not prove that the matcher ran.

## Scope

The matcher requires the expected Q8 output and two-sum yearly aggregation,
seven inner joins, the `MEDIUM BURNISHED COPPER` part predicate, long part keys,
and the measured projected columns. It rejects a target join with existing hints,
null-safe equality, extra join conditions, nontrivial child plans or unsafe
expressions. Unmatched plans are unchanged. No SQL rewrite, semi join or key
deduplication is introduced.

This is a query-specific experiment, not a general broadcast policy. There is
no general cost or broadcast-memory guard yet. Data growth, more executors and
other cluster topologies can increase replication and memory costs. Benchmark the
same input, SQL and cluster configuration in off/on/off arms and validate every
result. Include all 22 queries, including the original Q15 statements, when
reporting full-suite time. Do not attribute a combined Q5/Q8 result to this change
alone.

## Tests

`org.apache.gluten.experimental.q8.Q8EarlyBroadcastSuite` is under
`gluten-ut/test`. It uses Parquet fixtures and Spark's execution path to check
results and actual broadcast activation, including AQE on/off, duplicate keys,
nulls, decimals, date boundaries, an empty build selection, disabled/re-enabled
settings, existing user hints and nonmatching plans. These rule tests do not
replace native Gluten result validation or performance measurement.

After the documented native and JVM build, run through the repository wrapper:

```bash
./build/mvn test -pl gluten-ut/test -Pspark-ut -Pbackends-velox -Pspark-4.0 -Pjava-17 -Pscala-2.13 -Dmaven.compiler.release=17 -DwildcardSuites=org.apache.gluten.experimental.q8.Q8EarlyBroadcastSuite
```
