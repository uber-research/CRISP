from typing import ClassVar

class SingletonWrapper:
    _instances: ClassVar[dict[type, object]] = {}

    def __init__(self, cls):
        self._cls = cls

    def __call__(self, *args, **kwargs):
        if self._cls not in self._instances:
            self._instances[self._cls] = self._cls(*args, **kwargs)
        return self._instances[self._cls]
