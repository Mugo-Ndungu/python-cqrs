Transactional outbox examples.

| File | When to open |
|------|----------------|
| `save_events_into_outbox.py` | Persist notification/ECST events in the outbox |
| `kafka_outboxed_event_producing.py` | Drain outbox → Kafka broker |
| `fastapi_outbox.py` | FastAPI + one-tx write + publisher stub (needs examples extra) |
| `protobuf_outbox.py` | Opt-in Protobuf produce + consume (isolated map, no Kafka) |

## Checking the outbox against Kafka

The producer marks a row produced after the broker accepts it. A crash between
the ack and the commit, a client retry, or a row that ran out of retries can
leave the table and the topic disagreeing. `reconcile` compares them:

```bash
python -m cqrs.cli.outbox reconcile     --database-url postgresql+asyncpg://user:pass@db/app     --bootstrap-servers kafka:9092     --topic orders --since 2026-11-24T00:00:00+00:00
```

It prints one JSON object with row ids under `missing`, `unrecorded`,
`duplicated`, `exhausted` and `unverifiable`, and exits 0 when the first four
are empty, 1 when they aren't, and 2 when it couldn't run. Nothing is written
unless you pass `--apply`, which sends missing rows back to the producer and
marks unrecorded ones produced. Rows older than what the topic still retains
are reported as `unverifiable` rather than `missing`.

`requeue --database-url URL --id 41 --id 42` gives rows that ran out of
retries a fresh budget. The same check is available in code as
`cqrs.outbox.reconciler.reconcile`.

Docs: https://mkdocs.python-cqrs.dev/latest/outbox/
