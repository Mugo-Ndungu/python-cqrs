"""In-memory outbox repository.

[AI Instruction]
Import ``MemoryOutboxRepository`` from this module. It is not an attribute of
``cqrs``. Each instance has its own id counter. ``PRODUCED`` updates the row
status and keeps the row. ``cqrs.outbox.mock.MockOutboxedEventRepository`` is
a different class and is unchanged.
"""

import dataclasses
import datetime
import typing

from cqrs.mapping import outbox as map
from cqrs.models.event import INotificationEvent
from cqrs.outbox import repository
from cqrs.serializers.json import JsonEventSerializer
from cqrs.serializers.protocol import EventCodec


def _local_naive(value: datetime.datetime) -> datetime.datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone().replace(tzinfo=None)


class MemoryOutboxRepository(repository.OutboxedEventRepository):
    """
    Process-local outbox.

    The id counter and the rows belong to the instance. A row marked
    ``PRODUCED`` stays in the repository with that status. ``get_many``
    returns ``NEW`` and ``NOT_PRODUCED`` rows that have failed fewer than
    ``MAX_FLUSH_COUNTER_VALUE`` sends, the same budget as the SQL outboxes.
    ``rollback`` drops rows added since the last ``commit``.
    """

    def __init__(
        self,
        *,
        serializer: EventCodec | None = None,
        event_map: map.OutboxedEventMap | None = None,
    ) -> None:
        self._counter = 0
        self._rows: dict[int, repository.OutboxedEvent] = {}
        self._flush_counters: dict[int, int] = {}
        self._created_at: dict[int, datetime.datetime] = {}
        self._staged: list[int] = []
        self._serializer = serializer or JsonEventSerializer()
        self._event_map = event_map

    def _resolve_codec(self, event_name: str) -> EventCodec:
        if self._event_map is not None:
            registered = self._event_map.get_serializer(event_name)
            if registered is not None:
                return registered
        return self._serializer

    def add(self, event: INotificationEvent) -> None:
        self._counter += 1
        row_id = self._counter
        codec = self._resolve_codec(event.event_name)
        self._rows[row_id] = repository.OutboxedEvent(
            id=row_id,
            event=event,
            topic=event.topic,
            status=repository.EventStatus.NEW,
            payload_bytes=codec.serialize(event),
            content_type=codec.content_type_for(event),
        )
        self._flush_counters[row_id] = 0
        self._created_at[row_id] = _local_naive(event.event_timestamp)
        self._staged.append(row_id)

    async def get_many(
        self,
        batch_size: int = 100,
        topic: typing.Text | None = None,
    ) -> typing.List[repository.OutboxedEvent]:
        selectable = [
            row
            for row in self._rows.values()
            if row.status in (repository.EventStatus.NEW, repository.EventStatus.NOT_PRODUCED)
            and self._flush_counters[row.id] < repository.MAX_FLUSH_COUNTER_VALUE
            and (topic is None or row.topic == topic)
        ]
        selectable.sort(key=lambda row: row.id)
        return selectable[:batch_size]

    async def update_status(
        self,
        outboxed_event_id: int,
        new_status: repository.EventStatus,
    ) -> None:
        current = self._rows.get(outboxed_event_id)
        if current is None:
            return
        self._rows[outboxed_event_id] = dataclasses.replace(current, status=new_status)
        if new_status is repository.EventStatus.NOT_PRODUCED:
            self._flush_counters[outboxed_event_id] += 1

    async def list_rows(
        self,
        *,
        topics: typing.Sequence[str],
        since: datetime.datetime,
    ) -> typing.List[repository.OutboxRow]:
        wanted = set(topics)
        since = _local_naive(since)
        return [
            repository.OutboxRow(
                id=row.id,
                event_id=row.event.event_id,
                event_name=row.event.event_name,
                topic=row.topic,
                status=row.status,
                flush_counter=self._flush_counters[row.id],
                created_at=self._created_at[row.id],
            )
            for row in sorted(self._rows.values(), key=lambda row: row.id)
            if row.topic in wanted and self._created_at[row.id] >= since
        ]

    def _reset(self, ids: typing.Sequence[int], *, produced: bool) -> typing.List[int]:
        changed = []
        for row_id in sorted(set(ids)):
            current = self._rows.get(row_id)
            if current is None:
                continue
            wanted = repository.EventStatus.PRODUCED if produced else repository.EventStatus.NOT_PRODUCED
            if current.status is not wanted:
                continue
            self._rows[row_id] = dataclasses.replace(current, status=repository.EventStatus.NEW)
            self._flush_counters[row_id] = 0
            changed.append(row_id)
        return changed

    async def republish(self, ids: typing.Sequence[int]) -> typing.List[int]:
        return self._reset(ids, produced=True)

    async def requeue(self, ids: typing.Sequence[int]) -> typing.List[int]:
        return self._reset(ids, produced=False)

    async def commit(self) -> None:
        self._staged.clear()

    async def rollback(self) -> None:
        for row_id in self._staged:
            self._rows.pop(row_id, None)
            self._flush_counters.pop(row_id, None)
            self._created_at.pop(row_id, None)
        self._staged.clear()


__all__ = ("MemoryOutboxRepository",)
