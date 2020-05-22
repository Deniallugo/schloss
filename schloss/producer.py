import asyncio
import logging
from socket import gaierror
from typing import Optional

import aiokafka
from kafka.errors import KafkaConnectionError

logger = logging.getLogger(__name__)


class SchlossProducer:
    def __init__(self, url, **kwargs):
        loop = asyncio.get_running_loop()
        self.producer = aiokafka.AIOKafkaProducer(
            loop=loop, bootstrap_servers=url, **kwargs
        )
        self._start_task = None

    async def start(self):
        if self._start_task is not None:
            raise ValueError('Producer is already started')
        self._start_task = asyncio.create_task(self._start())

    async def _start(self):
        timeout = 2
        while True:
            try:
                await self.producer.start()
            except (KafkaConnectionError, gaierror):
                await asyncio.sleep(timeout)
                timeout *= 2
            else:
                break

    async def stop(self):
        if self._start_task is None:
            raise ValueError('Producer is not started')
        self._start_task.cancel()
        await self.producer.stop()
        self.started = False

    async def _wait_started(self):
        if self._start_task is None:
            raise ValueError('Producer is not started')
        await self._start_task

    async def send(self, topic: str, message: Optional[bytes], **kwargs):
        await self._wait_started()
        if not (message is None or isinstance(message, bytes)):
            raise TypeError('Message must be bytes')
        await self.producer.send_and_wait(topic, message, **kwargs)
        logger.info(f'Message on the topic {topic!r} was sent')
