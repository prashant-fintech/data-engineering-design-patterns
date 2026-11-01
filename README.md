# Data engineering design patterns

Companion code for the
[data engineering design patterns series](https://singhprashant.in/writing/series/data-engineering-design-patterns)
on singhprashant.in. Each part opens on a concrete failure, shows the
pattern that prevents it, and the variant of that pattern that still
breaks. The code runs on Spark with Delta Lake, Kafka, Airflow and
Postgres, all in Docker.

Each part gets its own folder with a README, a `docker-compose.yml`, and
scripts you run in order. Every step writes its result to disk, so you can
look at the table between steps instead of trusting a final number.

| Part | Post | Folder |
|---|---|---|
| 1 | [Idempotent loads](https://singhprashant.in/writing/data-engineering-design-patterns-01-idempotent-loads) | [`part-01-idempotent-loads`](part-01-idempotent-loads) |
| 2 | [Incremental loads and watermarks](https://singhprashant.in/writing/data-engineering-design-patterns-02-incremental-loads-watermarks) | [`part-02-incremental-loads`](part-02-incremental-loads) |

## Reference

The series uses Bartosz Konieczny's *Data Engineering Design Patterns*
(O'Reilly, 2025) as its reference. The code here is written independently
of [the book's own repository](https://github.com/bartosz25/data-engineering-design-patterns-book).

## Licence

The code is released under the [MIT Licence](LICENSE).
