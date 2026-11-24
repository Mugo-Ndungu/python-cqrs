import dataclasses
import datetime
import json
import os
import tempfile
import typing
import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

import cqrs
from cqrs.cli import outbox as cli
from cqrs.mapping.outbox import OutboxedEventMap
from cqrs.message_brokers import protocol as broker_protocol
from cqrs.outbox import reconciler
from cqrs.outbox.memory import MemoryOutboxRepository
from cqrs.outbox.reconciler import LogRecord, TopicContents, identify, reconcile
from cqrs.outbox.repository import MAX_FLUSH_COUNTER_VALUE, EventStatus
from cqrs.outbox.sqlalchemy import SqlAlchemyOutboxedEventRepository
from cqrs.serializers.default import message_wire_bytes

EVENT_NAME = "order_placed"
TOPIC = "orders"

SQLITE_OUTBOX_DDL = """
CREATE TABLE outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id BLOB NOT NULL,
    event_id_bin BLOB NOT NULL,
    event_status VARCHAR(12) NOT NULL DEFAULT 'NEW',
    flush_counter SMALLINT NOT NULL DEFAULT 0,
    event_name VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload BLOB NOT NULL,
    UNIQUE (event_id_bin, event_name)
)
"""


@dataclasses.dataclass
class Order:
    number: int


class OrderPlaced(cqrs.NotificationEvent):
    event_name: str
    payload: Order


def _event(number: int, *, topic: str = TOPIC, name: str = EVENT_NAME, **fields: typing.Any) -> OrderPlaced:
    return OrderPlaced(event_name=name, topic=topic, payload=Order(number=number), **fields)


def _event_map(*names: str) -> OutboxedEventMap:
    event_map = OutboxedEventMap()
    for name in names or (EVENT_NAME,):
        event_map.register(name, OrderPlaced)
    return event_map


def _since() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)


class Topics:
    """Stands in for both the broker and the topic reader."""

    def __init__(self) -> None:
        self.records: list[LogRecord] = []
        self.truncated_until: dict[str, datetime.datetime] = {}

    async def read(self, topics: typing.Sequence[str], since: datetime.datetime) -> TopicContents:
        return TopicContents(
            records=[record for record in self.records if record.topic in topics],
            truncated_until=dict(self.truncated_until),
        )

    def copies(self, event: OrderPlaced) -> int:
        return sum(1 for record in self.records if identify(record) == (event.event_id, event.event_name))


class Broker(broker_protocol.MessageBroker):
    def __init__(self, topics: Topics, *, failing: bool = False) -> None:
        self.topics = topics
        self.failing = failing

    async def send_message(self, message: broker_protocol.Message) -> None:
        if self.failing:
            raise ConnectionError("broker down")
        self.topics.records.append(
            LogRecord(
                topic=message.topic,
                value=message_wire_bytes(message),
                headers=tuple((message.headers or {}).items()),
            ),
        )


async def drain(repository: typing.Any, broker: Broker) -> int:
    producer = cqrs.EventProducer(broker, repository)
    rows = await repository.get_many(1000)
    for row in rows:
        await producer.send_message(row)
    await repository.commit()
    return len(rows)


@pytest.fixture
def sqlite_path() -> typing.Iterator[str]:
    descriptor, path = tempfile.mkstemp(suffix="-outbox.sqlite")
    os.close(descriptor)
    yield path
    os.remove(path)


@pytest.fixture(params=["memory", "sqlite"])
async def repository(request: pytest.FixtureRequest, sqlite_path: str) -> typing.AsyncIterator[typing.Any]:
    if request.param == "memory":
        yield MemoryOutboxRepository(event_map=_event_map(EVENT_NAME, "order_cancelled"))
        return
    engine = create_async_engine(f"sqlite+aiosqlite:///{sqlite_path}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(SQLITE_OUTBOX_DDL)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield SqlAlchemyOutboxedEventRepository(session, event_map=_event_map(EVENT_NAME, "order_cancelled"))
    await engine.dispose()


async def _rows(repository: typing.Any) -> list:
    return await repository.list_rows(topics=[TOPIC, "billing"], since=_since() - datetime.timedelta(days=1))


async def test_clean_history_reports_nothing(repository: typing.Any) -> None:
    topics = Topics()
    for number in range(3):
        repository.add(_event(number))
    await repository.commit()
    await drain(repository, Broker(topics))

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)

    assert report.to_dict() == {
        "missing": [],
        "unrecorded": [],
        "duplicated": [],
        "exhausted": [],
        "unverifiable": [],
    }
    assert report.clean


async def test_missing_row_is_republished_once(repository: typing.Any) -> None:
    topics = Topics()
    lost, kept = _event(1), _event(2)
    repository.add(lost)
    repository.add(kept)
    await repository.commit()
    await repository.update_status(1, EventStatus.PRODUCED)
    await repository.commit()
    await drain(repository, Broker(topics))

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics, apply=True)
    assert report.missing == [1]
    assert not report.clean

    await drain(repository, Broker(topics))
    await drain(repository, Broker(topics))
    assert topics.copies(lost) == 1
    assert topics.copies(kept) == 1
    assert (await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)).clean


async def test_unrecorded_row_is_marked_without_resending(repository: typing.Any) -> None:
    topics = Topics()
    event = _event(1)
    repository.add(event)
    await repository.commit()
    # The broker took it but the status update never committed.
    row = (await repository.get_many(10))[0]
    await Broker(topics).send_message(
        broker_protocol.Message(
            message_name=row.event.event_name,
            message_id=row.event.event_id,
            topic=row.topic,
            payload=row.event.to_dict(),
            payload_bytes=row.payload_bytes,
        ),
    )
    await repository.rollback()

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics, apply=True)
    assert report.unrecorded == [1]

    assert await drain(repository, Broker(topics)) == 0
    assert topics.copies(event) == 1
    assert (await _rows(repository))[0].status is EventStatus.PRODUCED


async def test_duplicates_are_reported_and_left_alone(repository: typing.Any) -> None:
    topics = Topics()
    event = _event(1)
    repository.add(event)
    repository.add(_event(2))
    await repository.commit()
    await drain(repository, Broker(topics))
    topics.records.append(topics.records[0])
    before = await _rows(repository)

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics, apply=True)

    assert report.duplicated == [1]
    assert report.missing == report.unrecorded == []
    assert await _rows(repository) == before


async def test_exhausted_rows_are_reported_then_requeued(repository: typing.Any) -> None:
    topics = Topics()
    event = _event(1)
    repository.add(event)
    await repository.commit()

    for _ in range(MAX_FLUSH_COUNTER_VALUE - 1):
        assert await drain(repository, Broker(topics, failing=True)) == 1
    assert (await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)).exhausted == []
    assert await drain(repository, Broker(topics, failing=True)) == 1
    assert await repository.get_many(10) == []

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics, apply=True)
    assert report.exhausted == [1]
    assert await repository.get_many(10) == []

    assert await repository.requeue([1]) == [1]
    await repository.commit()
    await drain(repository, Broker(topics))
    assert topics.copies(event) == 1
    assert (await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)).clean


async def test_requeue_skips_produced_rows(repository: typing.Any) -> None:
    repository.add(_event(1))
    await repository.commit()
    await drain(repository, Broker(Topics()))

    assert await repository.requeue([1, 99]) == []


async def test_dry_run_changes_nothing(repository: typing.Any) -> None:
    topics = Topics()
    for number in range(4):
        repository.add(_event(number))
    await repository.commit()
    await repository.update_status(1, EventStatus.PRODUCED)
    await repository.update_status(2, EventStatus.NOT_PRODUCED)
    await repository.commit()
    before = await _rows(repository)

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)

    assert report.missing == [1]
    assert await _rows(repository) == before


async def test_retention_gap_makes_old_rows_unverifiable(repository: typing.Any) -> None:
    topics = Topics()
    now = datetime.datetime.now()
    repository.add(_event(1, event_timestamp=now - datetime.timedelta(minutes=10)))
    repository.add(_event(2, event_timestamp=now))
    await repository.commit()
    await repository.update_status(1, EventStatus.PRODUCED)
    await repository.update_status(2, EventStatus.PRODUCED)
    await repository.commit()
    topics.truncated_until[TOPIC] = (now - datetime.timedelta(minutes=5)).astimezone()

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics, apply=True)

    assert report.unverifiable == [1]
    assert report.missing == [2]
    assert (await _rows(repository))[0].status is EventStatus.PRODUCED


async def test_event_is_identified_by_id_and_name(repository: typing.Any) -> None:
    topics = Topics()
    shared = uuid.uuid4()
    placed = _event(1, event_id=shared)
    cancelled = _event(1, name="order_cancelled", event_id=shared)
    repository.add(placed)
    repository.add(cancelled)
    await repository.commit()
    await drain(repository, Broker(topics))
    topics.records = [record for record in topics.records if identify(record) != (shared, "order_cancelled")]

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)

    assert report.missing == [2]


async def test_other_topics_and_older_rows_are_ignored(repository: typing.Any) -> None:
    topics = Topics()
    repository.add(_event(1, topic="billing"))
    repository.add(_event(2, event_timestamp=datetime.datetime.now() - datetime.timedelta(days=2)))
    repository.add(_event(3))
    await repository.commit()
    for row_id in (1, 2, 3):
        await repository.update_status(row_id, EventStatus.PRODUCED)
    await repository.commit()

    report = await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)

    assert report.missing == [3]


async def test_since_offset_is_respected(repository: typing.Any) -> None:
    topics = Topics()
    boundary = datetime.datetime.now().replace(microsecond=0)
    repository.add(_event(1, event_timestamp=boundary - datetime.timedelta(seconds=1)))
    repository.add(_event(2, event_timestamp=boundary))
    await repository.commit()
    await repository.update_status(1, EventStatus.PRODUCED)
    await repository.update_status(2, EventStatus.PRODUCED)
    await repository.commit()
    since = boundary.astimezone().astimezone(datetime.timezone(datetime.timedelta(hours=3)))

    report = await reconcile(repository, topics=[TOPIC], since=since, log=topics)

    assert report.missing == [2]


async def test_naive_since_is_rejected(repository: typing.Any) -> None:
    with pytest.raises(ValueError):
        await reconcile(repository, topics=[TOPIC], since=datetime.datetime.now(), log=Topics())


async def test_backends_agree_on_the_same_history(sqlite_path: str) -> None:
    async def run(repository: typing.Any) -> dict:
        topics = Topics()
        now = datetime.datetime.now()
        for number in range(8):
            repository.add(_event(number, event_timestamp=now - datetime.timedelta(minutes=8 - number)))
        await repository.commit()
        await repository.update_status(1, EventStatus.PRODUCED)
        await repository.commit()
        for _ in range(MAX_FLUSH_COUNTER_VALUE):
            await repository.update_status(2, EventStatus.NOT_PRODUCED)
        await repository.commit()
        await drain(repository, Broker(topics))
        topics.records.append(topics.records[-1])
        await repository.update_status(4, EventStatus.NEW)
        await repository.commit()
        return (await reconcile(repository, topics=[TOPIC], since=_since(), log=topics)).to_dict()

    memory = await run(MemoryOutboxRepository(event_map=_event_map()))

    engine = create_async_engine(f"sqlite+aiosqlite:///{sqlite_path}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(SQLITE_OUTBOX_DDL)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        sql = await run(SqlAlchemyOutboxedEventRepository(session, event_map=_event_map()))
    await engine.dispose()

    assert memory == sql
    assert memory["missing"] == [1]
    assert memory["exhausted"] == [2]
    assert memory["unrecorded"] == [4]
    assert memory["duplicated"] == [8]


async def test_unregistered_event_names_are_read_without_touching_rows(sqlite_path: str) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{sqlite_path}")
    async with engine.begin() as connection:
        await connection.exec_driver_sql(SQLITE_OUTBOX_DDL)
    async with AsyncSession(engine, expire_on_commit=False) as session:
        writer = SqlAlchemyOutboxedEventRepository(session, event_map=_event_map())
        writer.add(_event(1))
        await writer.commit()
        await writer.update_status(1, EventStatus.PRODUCED)
        await writer.commit()

        reader = SqlAlchemyOutboxedEventRepository(session, event_map=OutboxedEventMap())
        report = await reconcile(reader, topics=[TOPIC], since=_since(), log=Topics())
        rows = await _rows(reader)

    await engine.dispose()
    assert report.missing == [1]
    assert rows[0].flush_counter == 0


def test_identify_reads_json_values_and_codec_headers() -> None:
    event = _event(1)
    from_json = LogRecord(topic=TOPIC, value=json.dumps(event.to_dict(), default=str).encode())
    from_headers = LogRecord(
        topic=TOPIC,
        value=b"\x0a\x03abc",
        headers=(("event_name", EVENT_NAME.encode()), ("message_id", str(event.event_id).encode())),
    )

    assert identify(from_json) == (event.event_id, EVENT_NAME)
    assert identify(from_headers) == (event.event_id, EVENT_NAME)
    assert identify(LogRecord(topic=TOPIC, value=b"not json")) is None
    assert identify(LogRecord(topic=TOPIC, value=b'{"event_id": "nope", "event_name": "x"}')) is None
    assert identify(LogRecord(topic=TOPIC, value=None)) is None


class TestCli:
    @pytest.fixture
    async def database(self, sqlite_path: str) -> str:
        url = f"sqlite+aiosqlite:///{sqlite_path}"
        engine = create_async_engine(url)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(SQLITE_OUTBOX_DDL)
        async with AsyncSession(engine) as session:
            repository = SqlAlchemyOutboxedEventRepository(session, event_map=_event_map())
            repository.add(_event(1))
            repository.add(_event(2))
            await repository.commit()
            await repository.update_status(1, EventStatus.PRODUCED)
            for _ in range(MAX_FLUSH_COUNTER_VALUE):
                await repository.update_status(2, EventStatus.NOT_PRODUCED)
            await repository.commit()
        await engine.dispose()
        return url

    @pytest.fixture(autouse=True)
    def empty_topics(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(reconciler, "KafkaTopicLog", lambda bootstrap_servers: Topics())

    def _reconcile_args(self, database: str, *extra: str) -> list[str]:
        since = _since().isoformat()
        return [
            "reconcile",
            "--database-url",
            database,
            "--bootstrap-servers",
            "kafka:9092",
            "--topic",
            TOPIC,
            "--since",
            since,
            *extra,
        ]

    def test_reports_drift_with_exit_one(self, database: str, capsys: pytest.CaptureFixture[str]) -> None:
        assert cli.main(self._reconcile_args(database)) == cli.EXIT_DRIFT
        assert json.loads(capsys.readouterr().out) == {
            "missing": [1],
            "unrecorded": [],
            "duplicated": [],
            "exhausted": [2],
            "unverifiable": [],
        }

    def test_apply_then_requeue(self, database: str, capsys: pytest.CaptureFixture[str]) -> None:
        assert cli.main(self._reconcile_args(database, "--apply")) == cli.EXIT_DRIFT
        capsys.readouterr()

        assert cli.main(["requeue", "--database-url", database, "--id", "1", "--id", "2"]) == cli.EXIT_CLEAN
        assert json.loads(capsys.readouterr().out) == {"requeued": [2], "skipped": [1]}

        # Both rows are now NEW and waiting for the producer, which is not drift.
        assert cli.main(self._reconcile_args(database)) == cli.EXIT_CLEAN

    def test_bad_arguments_exit_two(self, database: str) -> None:
        assert cli.main(["reconcile", "--database-url", database]) == cli.EXIT_ERROR
        naive = [
            "reconcile",
            "--database-url",
            database,
            "--bootstrap-servers",
            "k",
            "--topic",
            TOPIC,
            "--since",
            "2026-01-01T00:00:00",
        ]
        assert cli.main(naive) == cli.EXIT_ERROR

    def test_unreachable_database_exits_two(self, tmp_path: typing.Any) -> None:
        url = f"sqlite+aiosqlite:///{tmp_path}/missing/outbox.sqlite"
        assert cli.main(self._reconcile_args(url)) == cli.EXIT_ERROR

    def test_since_accepts_z_suffix(self) -> None:
        parsed = cli._timestamp("2026-11-24T10:00:00Z")
        assert parsed == datetime.datetime(2026, 11, 24, 10, tzinfo=datetime.timezone.utc)


class TopicPartition(typing.NamedTuple):
    topic: str
    partition: int


@dataclasses.dataclass
class StoredRecord:
    offset: int
    timestamp: int
    value: bytes
    headers: tuple = ()


class OffsetAndTimestamp(typing.NamedTuple):
    offset: int
    timestamp: int


class FakeConsumer:
    """Just enough of AIOKafkaConsumer to drive KafkaTopicLog._read."""

    def __init__(self, logs: dict[TopicPartition, list[StoredRecord]]) -> None:
        self.logs = logs
        self.positions: dict[TopicPartition, int] = {}
        self.assigned: list[TopicPartition] = []
        self.paused: set[TopicPartition] = set()
        # Offsets of partitions that retention has emptied completely.
        self.emptied: dict[TopicPartition, int] = {}

    async def topics(self) -> set[str]:
        return {tp.topic for tp in self.logs}

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {tp.partition for tp in self.logs if tp.topic == topic}

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.assigned = list(partitions)
        self.positions = {tp: self._earliest(tp) for tp in partitions}

    def _earliest(self, tp: TopicPartition) -> int:
        log = self.logs[tp]
        return log[0].offset if log else self.emptied.get(tp, 0)

    def _end(self, tp: TopicPartition) -> int:
        log = self.logs[tp]
        return log[-1].offset + 1 if log else self.emptied.get(tp, 0)

    async def beginning_offsets(self, partitions: list[TopicPartition]) -> dict:
        return {tp: self._earliest(tp) for tp in partitions}

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict:
        return {tp: self._end(tp) for tp in partitions}

    async def offsets_for_times(self, query: dict) -> dict:
        found = {}
        for tp, ms in query.items():
            hit = next((record for record in self.logs[tp] if record.timestamp >= ms), None)
            found[tp] = None if hit is None else OffsetAndTimestamp(hit.offset, hit.timestamp)
        return found

    def seek(self, tp: TopicPartition, offset: int) -> None:
        self.positions[tp] = offset

    def pause(self, *partitions: TopicPartition) -> None:
        self.paused.update(partitions)

    def resume(self, *partitions: TopicPartition) -> None:
        self.paused.difference_update(partitions)

    async def position(self, tp: TopicPartition) -> int:
        return self.positions[tp]

    async def getmany(self, *partitions: TopicPartition, timeout_ms: int = 0, max_records: int | None = None) -> dict:
        batch = {}
        for tp in partitions or self.assigned:
            if tp in self.paused:
                continue
            ready = [record for record in self.logs[tp] if record.offset >= self.positions[tp]][:2]
            if max_records is not None:
                ready = ready[:max_records]
            if ready:
                self.positions[tp] = ready[-1].offset + 1
                batch[tp] = ready
        return batch


def _ms(value: datetime.datetime) -> int:
    return int(value.timestamp() * 1000)


async def test_kafka_reader_finds_records_and_retention_gaps() -> None:
    now = datetime.datetime.now(datetime.timezone.utc)
    events = [_event(number) for number in range(5)]

    def stored(offset: int, event: OrderPlaced, age_minutes: int) -> StoredRecord:
        value = json.dumps(event.to_dict(), default=str).encode()
        return StoredRecord(offset, _ms(now - datetime.timedelta(minutes=age_minutes)), value)

    logs = {
        TopicPartition(TOPIC, 0): [stored(0, events[0], 120), stored(1, events[1], 30), stored(2, events[2], 2)],
        # Retention already dropped offsets 0-6 here; the first survivor is 20 minutes old.
        TopicPartition(TOPIC, 1): [stored(7, events[3], 20), stored(8, events[4], 1)],
        TopicPartition(TOPIC, 2): [],
        TopicPartition("billing", 0): [stored(0, _event(9, topic="billing"), 1)],
    }
    consumer = FakeConsumer(logs)
    log = reconciler.KafkaTopicLog("unused", clock_skew=datetime.timedelta(minutes=5))

    contents = await log._read(consumer, TopicPartition, [TOPIC], now - datetime.timedelta(hours=1))

    found = {identify(record) for record in contents.records}
    assert found == {(event.event_id, EVENT_NAME) for event in events[1:]}
    assert contents.truncated_until[TOPIC] == datetime.datetime.fromtimestamp(
        _ms(now - datetime.timedelta(minutes=20)) / 1000,
        datetime.timezone.utc,
    )


async def test_kafka_reader_treats_an_emptied_partition_as_a_gap_up_to_now() -> None:
    consumer = FakeConsumer({TopicPartition(TOPIC, 0): []})
    consumer.emptied[TopicPartition(TOPIC, 0)] = 12
    before = datetime.datetime.now(datetime.timezone.utc)

    contents = await reconciler.KafkaTopicLog("unused")._read(consumer, TopicPartition, [TOPIC], before)

    assert contents.records == []
    assert contents.truncated_until[TOPIC] >= before
