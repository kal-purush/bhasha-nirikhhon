from asyncio import CancelledError, Task, gather
from contextlib import suppress
from contextvars import ContextVar
from logging import getLogger
from typing import Any, ClassVar, Dict, Tuple, Type

BindingMeta = Tuple[object, str]
TaskKey = Tuple[str, int | None, str | None]

LOG = getLogger("coil.runtime")
current_runtime: ContextVar["Runtime"] = ContextVar("current_runtime")


class Runtime:
    """
    A coil runtime

    This object is a context manager capable of managing tasks by an id
    (which may or may not be scoped to a binding). When the context exits,
    all tasks which are remaining are cancelled and then awaited.
    """

    __tasks: Dict[TaskKey, Task[Any]]
    __registry: ClassVar[Dict[int, "Runtime"]] = {}

    async def __aenter__(self) -> "Runtime":
        self.__tasks = {}
        self.__registry[id(self)] = self
        self.__reset_token = current_runtime.set(self)
        return self

    async def __aexit__(
        self,
        __exc_type: Type[Exception],
        __exc_value: Exception,
        __traceback: Any,
    ) -> None:
        self.__registry.pop(id(self))
        current_runtime.reset(self.__reset_token)

        tasks = [task for task in self.__tasks.values()]
        del self.__tasks

        meta_task = gather(*tasks, return_exceptions=True)
        meta_task.cancel()

        with suppress(CancelledError):
            results = await meta_task

            for result in results:
                if isinstance(result, Exception):
                    LOG.error(
                        "Unhandled exception in registered task:",
                        exc_info=(type(result), result, result.__traceback__),
                    )

    def register(
        self, task: Task[Any], id: str, *, source: BindingMeta | None = None
    ) -> None:
        """Register a task by an id.

        :param task: an asyncio task to be monitored.
        :param id: a unique identifier for the task, which can be used to
                   look it up later with :meth:`find`.
        :param source: An optional binding to which the id is scoped.
        :raise ValueError: when another task with the same id / source
                             combination is already registered.
        :raise AttributeError: if called outside the Runtime's context manager.

        """
        task_key = self.__get_task_key(id, source)

        if task_key in self.__tasks and self.__tasks[task_key] is not task:
            raise ValueError(
                "Another task is already registered with this id."
            )

        self.__tasks[task_key] = task

    def find(
        self, id: str, *, source: BindingMeta | None = None
    ) -> Task[Any] | None:
        """Retrieve a :meth:`registered <register>` task."""
        task_key = self.__get_task_key(id, source)
        return self.__tasks.get(task_key)

    def forget(self, task: Task[Any]) -> None:
        """Purge a given task from the internal registry."""
        self.__tasks = {
            key: val for key, val in self.__tasks.items() if val != task
        }

    def __get_task_key(self, _id: str, _source: BindingMeta | None) -> TaskKey:
        return (
            (_id, None, None)
            if _source is None
            else (_id, id(_source[0]), _source[1])
        )


def runtime(*, ensure: bool = True) -> Runtime:
    """Retrieve the currently active :class:`Runtime`.

    If `ensure=True` (default) and no runtime is currently active,
    a new one is returned, but it is not activated. (Runtimes
    are activated when they are used as context managers, for
    the duration of the context block.)

    If `ensure=False` (default) and no runtime is currently
    active, then this raises a `RuntimeError`.

    Example::

        import asyncio
        import coil

        @coil.bindableclass
        class Box:
            value: int

        async def main():
            box = Box(10)
            task1 = asyncio.create_task(asyncio.sleep(1000))
            task2 = asyncio.create_task(asyncio.sleep(1000))

            async with coil.runtime() as rt:
                rt.register(task1, "some-task")
                rt.register(task2, "some-task", source=(box, "value"))

                assert rt.find("some-task") is task1
                assert rt.find("some-task", source=(box, "value")) is task2

                rt.forget(task1)
                assert rt.find("some-task") is None

            assert task1.done()
            assert task2.done()

        asyncio.run(main())

    """
    try:
        return (
            current_runtime.get(Runtime()) if ensure else current_runtime.get()
        )
    except LookupError:
        raise RuntimeError("No runtime is currently active.") from None