"""
Check the outbox against the topics it publishes to.

The outbox is the source of truth for what should have been published. The
producer marks a row ``PRODUCED`` after the broker accepts it, but a crash
between the ack and the commit, a client-side retry or a row that runs out of
retries leaves the table and the topic disagreeing. ``reconcile`` reads both
and reports where they differ, and with ``apply=True`` repairs what can be
repaired from the outbox side.

The Kafka reader needs the ``kafka`` extra. It is imported only when a topic
is actually read.
"""

import collections
import dataclasses
import datetime
import logging
import typing
import uuid

import orjson

from cqrs.outbox import repository as outbox_repository

logger = logging.getLogger("cqrs")

EventKey = typing.Tuple[str, uuid.UUID, str]


@dataclasses.dataclass(frozen=True)
class LogRecord:
    """One record read from a topic. Header values may be bytes or str."""

    topic: str
    value: bytes | None
    headers: typing.Sequence[typing.Tuple[str, typing.Any]] = ()


@dataclasses.dataclass
class TopicContents:
    """
    What a read of the topics returned.

    ``truncated_until`` maps a topic to the moment before which records may
    already have been deleted by retention. Topics that still hold everything
    they were ever sent are absent.
    """

    records: typing.List[LogRecord] = dataclasses.field(default_factory=list)
    truncated_until: typing.Dict[str, datetime.datetime] = dataclasses.field(default_factory=dict)


class TopicLog(typing.Protocol):
    async def read(
        self,
        topics: typing.Sequence[str],
        since: datetime.datetime,
    ) -> TopicContents: ...


@dataclasses.dataclass
class ReconcileReport:
    missing: typing.List[int] = dataclasses.field(default_factory=list)
    unrecorded: typing.List[int] = dataclasses.field(default_factory=list)
    duplicated: typing.List[int] = dataclasses.field(default_factory=list)
    exhausted: typing.List[int] = dataclasses.field(default_factory=list)
    unverifiable: typing.List[int] = dataclasses.field(default_factory=list)

    @property
    def clean(self) -> bool:
        """True when nothing needs attention. Unverifiable rows don't count."""
        return not (self.missing or self.unrecorded or self.duplicated or self.exhausted)

    def to_dict(self) -> typing.Dict[str, typing.List[int]]:
        return {
            "missing": sorted(self.missing),
            "unrecorded": sorted(self.unrecorded),
            "duplicated": sorted(self.duplicated),
            "exhausted": sorted(self.exhausted),
            "unverifiable": sorted(self.unverifiable),
        }


def _text(value: typing.Any) -> str | None:
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, str):
        return value
    return None


def _uuid(value: typing.Any) -> uuid.UUID | None:
    if isinstance(value, uuid.UUID):
        return value
    text = _text(value)
    if text is None:
        return None
    try:
        return uuid.UUID(text)
    except ValueError:
        return None


def identify(record: LogRecord) -> typing.Tuple[uuid.UUID, str] | None:
    """
    Return ``(event_id, event_name)`` for a record, or ``None`` if it isn't
    an outbox event.

    Typed codecs such as protobuf put both in the headers. JSON records have
    no headers, so the ids are read back out of the value.
    """

    headers = {key: value for key, value in record.headers}
    event_id = _uuid(headers.get("message_id"))
    event_name = _text(headers.get("event_name"))
    if event_id is not None and event_name:
        return event_id, event_name

    if not record.value:
        return None
    try:
        body = orjson.loads(record.value)
    except orjson.JSONDecodeError:
        return None
    if not isinstance(body, dict):
        return None
    # Dataclass events serialize through dataclass-wizard in camelCase,
    # pydantic events keep the field names.
    event_id = _uuid(body.get("event_id", body.get("eventId")))
    event_name = body.get("event_name", body.get("eventName"))
    if event_id is None or not isinstance(event_name, str) or not event_name:
        return None
    return event_id, event_name


def _as_aware(value: datetime.datetime) -> datetime.datetime:
    # Naive timestamps in the outbox are local time; see OutboxRow.
    return value.astimezone() if value.tzinfo is None else value


class KafkaTopicLog:
    """
    Reads Kafka topics without joining a consumer group.

    Every partition is read from the first record at or after
    ``since - clock_skew`` up to the end offset taken when the read starts,
    so anything the producer had already published when the outbox was read
    is seen.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        clock_skew: datetime.timedelta = datetime.timedelta(minutes=5),
        poll_timeout_ms: int = 1000,
        **consumer_options: typing.Any,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._clock_skew = clock_skew
        self._poll_timeout_ms = poll_timeout_ms
        self._consumer_options = consumer_options

    async def read(
        self,
        topics: typing.Sequence[str],
        since: datetime.datetime,
    ) -> TopicContents:
        try:
            import aiokafka
        except ImportError:
            raise ImportError(
                "Reading Kafka topics needs 'aiokafka'. Install it with: pip install python-cqrs[kafka]",
            ) from None

        consumer = aiokafka.AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            **self._consumer_options,
        )
        await consumer.start()
        try:
            return await self._read(consumer, aiokafka.TopicPartition, topics, since)
        finally:
            await consumer.stop()

    async def _read(
        self,
        consumer: typing.Any,
        topic_partition: typing.Any,
        topics: typing.Sequence[str],
        since: datetime.datetime,
    ) -> TopicContents:
        existing = await consumer.topics()
        partitions = [
            topic_partition(topic, partition)
            for topic in topics
            if topic in existing
            for partition in sorted(consumer.partitions_for_topic(topic) or ())
        ]
        contents = TopicContents()
        if not partitions:
            return contents

        consumer.assign(partitions)
        earliest = await consumer.beginning_offsets(partitions)
        end = await consumer.end_offsets(partitions)
        read_started = datetime.datetime.now(datetime.timezone.utc)

        truncated = [tp for tp in partitions if earliest[tp] > 0]
        first_seen = await self._first_timestamps(
            consumer,
            [tp for tp in truncated if earliest[tp] < end[tp]],
            earliest,
        )
        for tp in truncated:
            # A partition that has been emptied by retention could have lost
            # anything up to now.
            boundary = first_seen.get(tp, read_started)
            current = contents.truncated_until.get(tp.topic)
            if current is None or boundary > current:
                contents.truncated_until[tp.topic] = boundary

        start_ms = int((since - self._clock_skew).timestamp() * 1000)
        found = await consumer.offsets_for_times({tp: max(start_ms, 0) for tp in partitions})
        start: typing.Dict[typing.Any, int] = {}
        for tp in partitions:
            located = found.get(tp)
            start[tp] = end[tp] if located is None else max(located.offset, earliest[tp])

        pending = {tp for tp in partitions if start[tp] < end[tp]}
        consumer.assign(sorted(pending, key=lambda tp: (tp.topic, tp.partition)))
        for tp in pending:
            consumer.seek(tp, start[tp])

        while pending:
            batch = await consumer.getmany(timeout_ms=self._poll_timeout_ms)
            for tp, records in batch.items():
                for record in records:
                    if record.offset >= end[tp]:
                        continue
                    contents.records.append(
                        LogRecord(topic=tp.topic, value=record.value, headers=tuple(record.headers or ())),
                    )
            # Compaction and transaction markers leave holes, so the last
            # record seen may sit below the end offset even when the
            # partition is done. The fetch position is what counts.
            for tp in list(pending):
                if await consumer.position(tp) >= end[tp]:
                    pending.discard(tp)
        return contents

    async def _first_timestamps(
        self,
        consumer: typing.Any,
        partitions: typing.List[typing.Any],
        earliest: typing.Dict[typing.Any, int],
    ) -> typing.Dict[typing.Any, datetime.datetime]:
        if not partitions:
            return {}
        consumer.assign(partitions)
        for tp in partitions:
            consumer.seek(tp, earliest[tp])
        seen: typing.Dict[typing.Any, datetime.datetime] = {}
        waiting = set(partitions)
        empty_polls = 0
        while waiting and empty_polls < 5:
            batch = await consumer.getmany(*waiting, timeout_ms=self._poll_timeout_ms, max_records=len(waiting))
            if not any(batch.values()):
                empty_polls += 1
                continue
            for tp, records in batch.items():
                if records and tp in waiting:
                    seen[tp] = datetime.datetime.fromtimestamp(records[0].timestamp / 1000, datetime.timezone.utc)
                    waiting.discard(tp)
                    consumer.pause(tp)
        consumer.resume(*partitions)
        return seen


async def reconcile(
    repository: outbox_repository.OutboxedEventRepository,
    *,
    topics: typing.Sequence[str],
    since: datetime.datetime,
    bootstrap_servers: str | None = None,
    apply: bool = False,
    log: TopicLog | None = None,
) -> ReconcileReport:
    """
    Compare outbox rows on ``topics`` created at or after ``since`` with the
    records on those topics.

    ``since`` must carry a UTC offset. Pass ``bootstrap_servers`` to read
    Kafka, or ``log`` to read from somewhere else.

    Without ``apply`` nothing is written. With it, missing rows go back to
    ``NEW`` so the producer sends them once more, unrecorded rows are marked
    ``PRODUCED``, and the repository is committed. Duplicates, exhausted rows
    and unverifiable rows are only reported.
    """

    if since.tzinfo is None:
        raise ValueError("since must include a UTC offset")
    if log is None:
        if bootstrap_servers is None:
            raise ValueError("pass bootstrap_servers or log")
        log = KafkaTopicLog(bootstrap_servers)

    wanted = sorted(set(topics))
    # Rows first, topics second: a row the outbox already shows as produced
    # was sent before the end offsets are taken, so its record is in range.
    rows = await repository.list_rows(topics=wanted, since=since)
    contents = await log.read(wanted, since)

    counts: typing.Counter[EventKey] = collections.Counter()
    unidentified = 0
    for record in contents.records:
        identity = identify(record)
        if identity is None:
            unidentified += 1
            continue
        counts[(record.topic, identity[0], identity[1])] += 1
    if unidentified:
        logger.info("outbox reconcile: %d records on %s are not outbox events", unidentified, ", ".join(wanted))

    report = ReconcileReport()
    for row in rows:
        seen = counts[(row.topic, row.event_id, row.event_name)]
        produced = row.status is outbox_repository.EventStatus.PRODUCED
        if seen > 1:
            report.duplicated.append(row.id)
        if seen and not produced:
            report.unrecorded.append(row.id)
        elif not seen and produced:
            boundary = contents.truncated_until.get(row.topic)
            if boundary is not None and _as_aware(row.created_at) < boundary:
                report.unverifiable.append(row.id)
            else:
                report.missing.append(row.id)
        elif not seen and row.exhausted:
            report.exhausted.append(row.id)

    if apply and (report.missing or report.unrecorded):
        for row_id in report.unrecorded:
            await repository.update_status(row_id, outbox_repository.EventStatus.PRODUCED)
        await repository.republish(report.missing)
        await repository.commit()

    return report


__all__ = (
    "KafkaTopicLog",
    "LogRecord",
    "ReconcileReport",
    "TopicContents",
    "TopicLog",
    "identify",
    "reconcile",
)
