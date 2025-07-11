from abc import ABC, abstractmethod
import asyncio

class Worker(ABC):
    @abstractmethod
    async def run(self) -> None:
        """
        Perform the worker’s main task.
        Should be overridden by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    def health_check(self) -> None:
        """
        Check the worker’s health (synchronous).
        Should be overridden by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    async def listen_task(self) -> None:
        """
        Listen for incoming tasks asynchronously.
        Should be overridden by subclasses.
        """
        raise NotImplementedError

