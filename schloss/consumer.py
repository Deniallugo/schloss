import asyncio
import logging
from socket import gaierror
from typing import Dict, Sequence

import aiokafka
from kafka.errors import KafkaConnectionError

from .dispatcher import KafkaDispatcher
from .session import SchlossSession
from .types import MessageTypeDispatcher, MiddlewareType

logger = logging.getLogger(__name__)


class SynchronousSchlossConsumer:

    def __init__(
            self, dependencies,
            url: str, group_id: str,
            handlers: Dict[str, MessageTypeDispatcher],
            middlewares: Sequence[MiddlewareType] = (),
            auto_offset_reset: str = 'earliest'
    ):
        self.dependencies = dependencies
        self.url = url
        self.group_id = group_id
        self.handlers = handlers
        self.kafka_dispatcher = KafkaDispatcher(
            handlers, middlewares=middlewares,
        )
        self.consume_task = None
        self.auto_offset_reset = auto_offset_reset

    async def consume(self):
        loop = asyncio.get_running_loop()

        topics = self.kafka_dispatcher.received_topics
        running_task = None
        start_consumer = True
        timeout = 2
        while True:
            consumer = aiokafka.AIOKafkaConsumer(
                *topics,
                loop=loop, bootstrap_servers=self.url,
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset=self.auto_offset_reset,
            )
            try:
                if start_consumer:
                    await consumer.start()
                    start_consumer = False
                logger.info('Kafka Consumer started')
                async for msg in consumer:
                    running_task = asyncio.create_task(
                        self.handle_msg(msg, consumer)
                    )
                    # Must be waited here, otherwise handling will run in
                    # parallel and will incorrectly commit offsets for not
                    # finished tasks
                    await asyncio.shield(running_task)
                    running_task = None

            except asyncio.CancelledError:
                if running_task:
                    await asyncio.wait({running_task})
                break
            except (gaierror, KafkaConnectionError):
                start_consumer = True
                await consumer.stop()
                await asyncio.sleep(timeout)
            except Exception as e:
                logger.exception(e)
                await asyncio.sleep(timeout)
            timeout *= 2

        await consumer.stop()

    async def handle_msg(self, msg, consumer):
        session = SchlossSession(msg=msg, dependencies=self.dependencies)
        logger.info(f'Consuming message on the topic {msg.topic!r}')
        await self.kafka_dispatcher.dispatch(session)
        await consumer.commit()

    async def start(self):
        self.consume_task = asyncio.create_task(self.consume())

    async def stop(self):
        self.consume_task.cancel()
        await asyncio.wait({self.consume_task}, timeout=100)
