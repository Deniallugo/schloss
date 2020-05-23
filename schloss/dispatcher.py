from functools import partial
import logging
from typing import Awaitable, Dict, Optional, Sequence

from .session import SchlossSession
from .types import MessageTypeDispatcher, MiddlewareType

logger = logging.getLogger(__name__)


class SchlossDispatcher:

    def __init__(
            self,
            handlers: Dict[str, MessageTypeDispatcher],
            middlewares: Sequence[MiddlewareType] = (),
    ):
        assert isinstance(handlers, dict)
        self._handlers = handlers
        self._middlewares = middlewares

    @property
    def received_topics(self):
        return self._handlers.keys()

    async def dispatch(self, session: SchlossSession):
        handler = self._handlers[session.topic]
        for middleware in self._middlewares:
            handler = partial(middleware, handler=handler)
        await handler(session)
