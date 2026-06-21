from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, List

log = logging.getLogger(__name__)

EventHandler = Callable[[Dict[str, Any]], Awaitable[None]]


class EventBus:
    """In-process pub/sub backed by asyncio queues.

    Sub-engines and websocket fan-out subscribe by event type. Publishes are
    fire-and-forget; slow subscribers get dropped messages rather than blocking
    the publisher.
    """

    def __init__(self, max_queue: int = 1000) -> None:
        self._subscribers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._handlers: Dict[str, List[EventHandler]] = defaultdict(list)
        self._max_queue = max_queue
        self._lock = asyncio.Lock()

    async def subscribe(self, event_type: str) -> asyncio.Queue:
        async with self._lock:
            q: asyncio.Queue = asyncio.Queue(maxsize=self._max_queue)
            self._subscribers[event_type].append(q)
            return q

    async def unsubscribe(self, event_type: str, queue: asyncio.Queue) -> None:
        async with self._lock:
            if queue in self._subscribers[event_type]:
                self._subscribers[event_type].remove(queue)

    def on(self, event_type: str, handler: EventHandler) -> None:
        """Register an async handler called inline on publish."""
        self._handlers[event_type].append(handler)

    async def publish(self, event_type: str, payload: Dict[str, Any]) -> None:
        # Fire registered handlers concurrently.
        handlers = list(self._handlers.get(event_type, []))
        if handlers:
            await asyncio.gather(
                *(self._safe_handler(h, payload) for h in handlers),
                return_exceptions=True,
            )

        # Deliver to subscribers; drop if their queue is full so publishers
        # never block on a stalled consumer.
        for q in list(self._subscribers.get(event_type, [])):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                log.warning(
                    "EventBus subscriber queue full for %s — dropping event",
                    event_type,
                )

    @staticmethod
    async def _safe_handler(handler: EventHandler, payload: Dict[str, Any]) -> None:
        try:
            await handler(payload)
        except Exception:
            log.exception("EventBus handler raised")
