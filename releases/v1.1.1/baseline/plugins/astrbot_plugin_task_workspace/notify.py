# -*- coding: utf-8 -*-
"""Durable notification outbox.

Notifications are persisted before delivery and de-duplicated on
``(run_id, event_key)``. Delivery uses at-least-once semantics because the
platform protocol provides no idempotency key. A failed send leaves the row
``pending`` for retry rather than dropping it.
"""
from __future__ import annotations

import asyncio

from astrbot.api import logger
from astrbot.api.event import MessageChain

from .models import OutboxItem, now


class Notifier:
    def __init__(self, store, context, interval: int = 10):
        self.store = store
        self.context = context
        self.interval = interval
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def enqueue(self, run_id: str, event_key: str, session: str, content: str) -> bool:
        item = OutboxItem(
            id=None,
            run_id=run_id,
            event_key=event_key,
            session=session,
            content=content,
            created_ts=now(),
        )
        inserted = await self.store.enqueue_outbox(item)
        if inserted:
            logger.info("[task-workspace] outbox enqueue run=%s key=%s", run_id, event_key)
        return inserted

    async def flush_once(self, sender=None, limit: int = 20) -> int:
        sent = 0
        for item in await self.store.claim_outbox(limit):
            ok, err = await self._deliver(item, sender)
            if ok:
                await self.store.mark_outbox_sent(item.id)
                sent += 1
            else:
                await self.store.mark_outbox_failed(item.id, err, item.attempts + 1)
        return sent

    async def _deliver(self, item: OutboxItem, sender=None) -> tuple[bool, str]:
        if not item.session:
            return False, "no session"
        try:
            if sender is not None:
                await sender(item.session, item.content)
            else:
                await self.context.send_message(
                    item.session, MessageChain().message(item.content)
                )
            return True, ""
        except Exception as e:
            logger.warning("[task-workspace] 通知发送失败 run=%s: %s", item.run_id, e)
            return False, str(e)

    def start(self):
        if self._task is None or self._task.done():
            self._stopped.clear()
            self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while not self._stopped.is_set():
            try:
                await self.flush_once()
            except Exception:
                logger.warning("[task-workspace] outbox 循环出错", exc_info=True)
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass

    async def stop(self):
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
