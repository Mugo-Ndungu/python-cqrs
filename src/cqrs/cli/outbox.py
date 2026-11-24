"""
Outbox maintenance commands.

    python -m cqrs.cli.outbox reconcile --database-url URL --bootstrap-servers HOSTS \\
        --topic orders --since 2026-11-24T00:00:00+00:00 [--apply]
    python -m cqrs.cli.outbox requeue --database-url URL --id 41 --id 42

``reconcile`` prints a JSON report and exits 0 when the outbox and the topics
agree, 1 when they don't, and 2 when it could not run. ``requeue`` prints the
ids it reset and the ids it left alone.

Needs the ``sqlalchemy`` extra, a driver for the database URL, and the
``kafka`` extra for ``reconcile``.
"""

import argparse
import asyncio
import datetime
import json
import sys
import typing

EXIT_CLEAN = 0
EXIT_DRIFT = 1
EXIT_ERROR = 2


def _timestamp(value: str) -> datetime.datetime:
    text = value.strip()
    # fromisoformat only learned to read a trailing Z in 3.11.
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not an ISO-8601 timestamp: {value!r}") from None
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"timestamp needs a UTC offset, e.g. {value}+00:00")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m cqrs.cli.outbox", description="Outbox maintenance.")
    commands = parser.add_subparsers(dest="command", required=True)

    reconcile = commands.add_parser("reconcile", help="compare outbox rows with what is on the topics")
    reconcile.add_argument("--database-url", required=True)
    reconcile.add_argument("--bootstrap-servers", required=True)
    reconcile.add_argument("--topic", dest="topics", action="append", required=True)
    reconcile.add_argument("--since", type=_timestamp, required=True, help="ISO-8601 with a UTC offset")
    reconcile.add_argument("--apply", action="store_true", help="repair missing and unrecorded rows")
    reconcile.add_argument("--table", default=None, help="outbox table name if it was renamed")

    requeue = commands.add_parser("requeue", help="give failed rows a fresh retry budget")
    requeue.add_argument("--database-url", required=True)
    requeue.add_argument("--id", dest="ids", type=int, action="append", required=True)
    requeue.add_argument("--table", default=None, help="outbox table name if it was renamed")
    return parser


async def _with_repository(
    database_url: str,
    table: str | None,
    work: typing.Callable[[typing.Any], typing.Awaitable[typing.Any]],
) -> typing.Any:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from cqrs.outbox.sqlalchemy import OutboxModel, SqlAlchemyOutboxedEventRepository

    if table:
        OutboxModel.__table__.name = table  # type: ignore[attr-defined]

    engine = create_async_engine(database_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            return await work(SqlAlchemyOutboxedEventRepository(session))
    finally:
        await engine.dispose()


async def _reconcile(args: argparse.Namespace) -> int:
    from cqrs.outbox.reconciler import reconcile

    async def work(repository: typing.Any) -> typing.Any:
        return await reconcile(
            repository,
            topics=args.topics,
            since=args.since,
            bootstrap_servers=args.bootstrap_servers,
            apply=args.apply,
        )

    report = await _with_repository(args.database_url, args.table, work)
    print(json.dumps(report.to_dict()))
    return EXIT_CLEAN if report.clean else EXIT_DRIFT


async def _requeue(args: argparse.Namespace) -> int:
    async def work(repository: typing.Any) -> typing.List[int]:
        changed = await repository.requeue(args.ids)
        await repository.commit()
        return changed

    changed = await _with_repository(args.database_url, args.table, work)
    skipped = sorted(set(args.ids) - set(changed))
    print(json.dumps({"requeued": sorted(changed), "skipped": skipped}))
    return EXIT_CLEAN


def main(argv: typing.Sequence[str] | None = None) -> int:
    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exit_:
        return EXIT_ERROR if exit_.code else EXIT_CLEAN

    command = _reconcile if args.command == "reconcile" else _requeue
    try:
        return asyncio.run(command(args))
    except KeyboardInterrupt:
        return EXIT_ERROR
    except Exception as error:
        print(f"outbox {args.command}: {type(error).__name__}: {error}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
