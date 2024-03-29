from typing import Awaitable, Callable, Union

from schloss.session import SchlossSession

Message = object  # Protobuf message or something simillar
KafkaHandler = Callable[[SchlossSession], Union[Awaitable, None]]

MiddlewareType = Callable[[SchlossSession, KafkaHandler], Awaitable]
