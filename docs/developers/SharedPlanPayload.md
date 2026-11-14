# Sharing large native scan plans

The Velox backend can put large native scan plans in a Spark broadcast. Tasks carry a broadcast handle and their split metadata instead of serializing the same plan byte array for every task. This is disabled by default and does not change native plan parsing or query results.

| Setting | Default | Meaning |
| --- | --- | --- |
| `spark.gluten.experimental.sharedPlanPayload.enabled` | `false` | Enable sharing for the Velox scan partition path. |
| `spark.gluten.experimental.sharedPlanPayload.minBytes` | `262144` | Minimum serialized plan size to share. The allowed range is 262144 through 2147483647 bytes. |

For an isolated comparison, keep the native build, input files, SQL, planner settings and placement fixed, and change only `sharedPlanPayload.enabled`. Plans below the threshold retain the original partition path. `SHARED_PLAN_PAYLOAD` driver log lines record actual sharing; setting the flag does not prove a plan exceeded the threshold.

The driver copies the plan once before publishing it. Consumers must treat the resulting byte array as read-only. Each partition owns a broadcast reference, so repeated actions and task retries can still fetch the payload. There is no task-completion destroy; Spark's normal cleanup reclaims broadcasts when their owning objects are no longer reachable. This can retain memory longer than one query.

`SharedPlanPayloadSuite` under `gluten-ut/test` checks disabled and boundary paths, partition metadata, caller mutation, real task serialization, repeated actions and recomputation. Those tests use Spark transport without a native reader. They do not establish behavior under whole-executor loss, query cancellation or sustained concurrent workloads.
