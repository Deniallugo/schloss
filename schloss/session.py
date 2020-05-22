from collections.abc import MutableMapping

from aiokafka import ConsumerRecord


class SchlossSession(MutableMapping):

    def __init__(self, msg: ConsumerRecord, dependencies=None, state=None):
        self._message = msg
        self._state = state or {}
        self._dependencies = dependencies
        self._topic = msg.topic

    # MutableMapping API

    def __getitem__(self, key):
        return self._state[key]

    def __setitem__(self, key, value):
        self._state[key] = value

    def __delitem__(self, key):
        del self._state[key]

    def __len__(self):
        return len(self._state)

    def __iter__(self):
        return iter(self._state)

    #########

    @property
    def message(self):
        return self._message

    @property
    def topic(self):
        return self._topic

    @property
    def dependencies(self):
        return self._dependencies

