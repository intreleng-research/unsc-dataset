from abc import ABC, abstractmethod


class Job(ABC):
    """
    A job that can be queued to a `Queue` instance for retrying when failing.

    """

    def __init__(self, description: str = "") -> None:
        self.complete: bool = False
        self.attempts: int = 1
        self.description: str = description

    @abstractmethod
    def info(self):
        pass

    @property
    def complete(self):
        return self._complete

    @complete.setter
    def complete(self, val: bool):
        self._complete = val

    def __repr__(self) -> str:
        return f"Job complete status: {self.complete} -- Attempts: {self.attempts} -- Description: {self.description}"
