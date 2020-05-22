from functools import partial
from queue import Queue

import pytest

from schloss import KafkaDispatcher, SchlossSession


class Message:
    topic = 'test'


class Message2:
    topic = 'test'


async def simple_handler(session: SchlossSession, *, queue: Queue):
    queue.put_nowait(session.message)


async def unwrapped_handler(session, message, *, queue: Queue):
    queue.put_nowait(message)


async def middleware(session, handler):
    session['middleware_is_called'] = True
    return await handler(session)


@pytest.mark.asyncio
async def test_basic_kafka_dispatch():
    queue = Queue()
    handler = partial(simple_handler, queue=queue)
    dispatcher = KafkaDispatcher(
        handlers={
            'test': handler
        },
        middlewares=[middleware],
    )
    session = SchlossSession(
        Message()
    )
    await dispatcher.dispatch(session)
    assert isinstance(queue.get_nowait(), Message)
