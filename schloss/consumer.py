import asyncio
import logging
from socket import gaierror
from typing import Protocol

import aiokafka
from kafka.errors import KafkaConnectionError

from .session import SchlossSession
from .dispatcher import SchlossDispatcher
from .types import Message


class SessionCreator(Protocol):
    def __call__(self, msg: Message, **kwargs) -> 'SchlossSession': ...


logger = logging.getLogger(__name__)

class AttemptsFinished(Exception):
    pass

class SynchronousSchlossConsumer:

    def __init__(
        self,
        url: str, group_id: str,
        session_creator: SessionCreator,
        dispatcher: SchlossDispatcher,
        auto_offset_reset: str = 'earliest',
        options: dict = None
    ):
        self.url = url
        self.group_id = group_id
        self.dispatcher = dispatcher
        self.consume_task = None
        self._session_creator = session_creator
        self._aiokafka_options = options or {}
        self.auto_offset_reset = auto_offset_reset

    async def consume(self, attempts_count=100, initial_timeout=2, max_timeout=120):
        loop = asyncio.get_running_loop()

        topics = self.dispatcher.received_topics
        running_task = None
        start_consumer = True
        timeout = initial_timeout 
        consumer = aiokafka.AIOKafkaConsumer(
            *topics,
            loop=loop, bootstrap_servers=self.url,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.auto_offset_reset,
            **self._aiokafka_options
        )
        try:
            while True:
                try:
                    if start_consumer:
                        await consumer.start()
                        start_consumer = False
                        timeout = initial_timeout 
                        logger.info('Kafka Consumer started')
                    async for msg in consumer:
                        for _ in range(attempts_count):
                            try:
                                running_task = asyncio.create_task(
                                    self.handle_msg(msg, consumer)
                                )
                                # Must be waited here, otherwise handling will run in
                                # parallel and will incorrectly commit offsets for not
                                # finished tasks
                                await asyncio.shield(running_task)
                                running_task = None
                                timeout = initial_timeout 
                                break
                            except asyncio.CancelledError:
                                if running_task:
                                    await asyncio.wait({running_task}, timeout=timeout)
                                raise
                            except Exception as e:
                                logger.exception(e)
                                await asyncio.sleep(timeout)
                        else:
                            raise AttemptsFinished()

                except (gaierror, KafkaConnectionError):
                    start_consumer = True
                    await consumer.stop()
                    await asyncio.sleep(timeout)
                if timeout < max_timeout:
                    timeout *= 2
        finally:
            await consumer.stop()
            

    async def handle_msg(self, msg, consumer):
        session = self._session_creator(msg=msg)
        logger.info(f'Consuming message on the topic {msg.topic!r}')
        await self.dispatcher.dispatch(session)
        await consumer.commit()

    async def start(self):
        self.consume_task = asyncio.create_task(self.consume())

    async def stop(self):
        self.consume_task.cancel()
        await asyncio.wait({self.consume_task}, timeout=100)
