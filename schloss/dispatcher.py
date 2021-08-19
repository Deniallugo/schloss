from abc import ABC, abstractmethod
from functools import partial
import logging
from typing import Dict, List, Sequence

from .session import SchlossSession
from .types import KafkaHandler, MiddlewareType

logger = logging.getLogger(__name__)


class CustomDispatcher(ABC):
    @abstractmethod
    def dispatch(self, session) -> KafkaHandler:
        raise NotImplementedError

    @abstractmethod
    @property
    def received_topics(self) -> List[str]:
        raise NotImplementedError


class SchlossDispatcher:

    def __init__(
            self,
            handlers: Dict[str, KafkaHandler],
            middlewares: Sequence[MiddlewareType] = (),
            custom_dispatcher: CustomDispatcher = None
    ):
        assert isinstance(handlers, dict)
        self._handlers = handlers
        self._middlewares = middlewares
        self._custom_dispatcher = custom_dispatcher

    @property
    def received_topics(self):
        topics = list(self._handlers.keys())
        if self._custom_dispatcher is not None:
            topics.extend(self._custom_dispatcher.received_topics)
        return topics

    async def dispatch(self, session: SchlossSession):
        handler = self._handlers.get(session.topic)
        if handler is None and self._custom_dispatcher is not None:
            handler = self._custom_dispatcher.dispatch(session)
        for middleware in self._middlewares:
            handler = partial(middleware, handler=handler)
        await handler(session)
