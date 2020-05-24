from aiokafka import ConsumerRecord


class SchlossSession(dict):

    def __init__(self, msg: ConsumerRecord, dependencies=None, **kwargs):
        super().__init__(**kwargs)
        self._message = msg
        self._dependencies = dependencies
        self._topic = msg.topic

    @classmethod
    def new(cls, msg: ConsumerRecord, dependencies=None, **kwargs):
        return cls(msg, dependencies, **kwargs)

    @property
    def message(self):
        return self._message

    @property
    def topic(self):
        return self._topic

    @property
    def dependencies(self):
        return self._dependencies
