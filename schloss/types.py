from typing import Awaitable, Callable, Union

from schloss import SchlossSession

Message = object  # Protobuf message or something simillar
KafkaHandler = Callable[[SchlossSession], Union[Awaitable, None]]
MessageTypeDispatcher = KafkaHandler

MiddlewareType = Callable[[SchlossSession, KafkaHandler], Awaitable]
