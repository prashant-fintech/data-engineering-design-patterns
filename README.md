# Data Engineering Design Patterns

Companion code for the
[Data Engineering Design Patterns series](https://singhprashant.in/writing/series/data-engineering-design-patterns)
on singhprashant.in. Each part opens on a concrete failure, shows the
pattern that prevents it, and the variant of that pattern that still
breaks. The code runs on Spark with Delta Lake, Kafka, Airflow and
Postgres, all in Docker.

Each part gets its own folder with a README, a `docker-compose.yml`, and
scripts you run in order. Every step writes its result to disk, so you can
look at the table between steps instead of trusting a final number.

| Part | Post | Folder |
|---|---|---|
| 1 | [Idempotent Loads](https://singhprashant.in/writing/data-engineering-design-patterns-01-idempotent-loads) | [`part-01-idempotent-loads`](part-01-idempotent-loads) |
| 2 | [Incremental Loads and Watermarks](https://singhprashant.in/writing/data-engineering-design-patterns-02-incremental-loads-watermarks) | [`part-02-incremental-loads`](part-02-incremental-loads) |

## Licence

The code is released under the [MIT Licence](LICENSE).
