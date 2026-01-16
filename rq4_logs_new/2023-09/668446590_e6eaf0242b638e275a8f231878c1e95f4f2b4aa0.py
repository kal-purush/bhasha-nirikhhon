import multiprocessing as mpr
import threading as thr
from typing import Optional, Generator, Callable, Any, Generic, TypeVar, Iterator

TBatchParams = TypeVar('TBatchParams')
TBatchResult = TypeVar('TBatchResult')
TBatchWorker = Callable[[TBatchParams], TBatchResult]
TBatchParamsGenerator = Generator[TBatchParams, None, None]


class BatchParamsContainer(Generic[TBatchParams]):
    last_batch_id: int = -1
    batch_id: int
    worker_fn: TBatchWorker
    params: TBatchParams

    def __init__(self, worker_fn: TBatchWorker, params: TBatchParams):
        self.worker_fn = worker_fn
        BatchParamsContainer.last_batch_id += 1
        self.batch_id = BatchParamsContainer.last_batch_id
        self.params = params


class BatchResultContainer(Generic[TBatchResult]):
    batch_id: int
    result: Optional[TBatchResult]
    ex: Optional[Exception]

    def __init__(self, batch_id: int, result: Optional[TBatchResult] = None, ex: Optional[Exception] = None):
        self.batch_id = batch_id
        self.result = result
        self.ex = ex


def process_batch(params_cont: BatchParamsContainer) -> BatchResultContainer:
    try:
        result = params_cont.worker_fn(params_cont.params)
        return BatchResultContainer(params_cont.batch_id, result)
    except Exception as ex:
        return BatchResultContainer(params_cont.batch_id, ex=ex)


class BatchProcessor(Generic[TBatchParams, TBatchResult]):
    worker_fn: TBatchWorker
    id_to_params: dict[int, BatchParamsContainer[TBatchParams]]
    id_to_results: dict[int, BatchResultContainer[TBatchResult]]
    pool: mpr.Pool
    params_it: Iterator[TBatchParams]
    preserve_order: bool = False
    buffer_sz: int
    next_id_in_order: int = -1
    stopped: bool = False
    cv: thr.Condition

    def __init__(self, worker_fn: TBatchWorker, pool: mpr.Pool, params_it: Iterator[TBatchParams],
                 preserve_order: bool = False, buffer_sz: int = 3):
        self.worker_fn = worker_fn
        self.id_to_params = {}
        self.id_to_results = {}
        self.buffer_sz = buffer_sz
        self.pool = pool
        self.params_it = params_it
        self.preserve_order = preserve_order
        self.stopped = False
        self.cv = thr.Condition()
        self.sync_workers()

    def pool_cb(self, res_cont: BatchResultContainer):
        self.id_to_results[res_cont.batch_id] = res_cont
        del self.id_to_params[res_cont.batch_id]
        with self.cv:
            self.cv.notify()

    @staticmethod
    def pool_error_cb(err: Any):
        print('pool_error_cb:', err)

    def sync_workers(self):
        if self.stopped:
            return
        n_sent, n_received = len(self.id_to_params), len(self.id_to_results)
        for i in range(self.buffer_sz - n_sent - n_received):
            params = next(self.params_it, None)
            if params is None:
                break
            params_cont = BatchParamsContainer(self.worker_fn, params)
            if self.preserve_order:
                if self.next_id_in_order < 0:
                    self.next_id_in_order = params_cont.batch_id
            self.id_to_params[params_cont.batch_id] = params_cont
            self.pool.apply_async(process_batch, (params_cont,), callback=self.pool_cb, error_callback=self.pool_error_cb)

    def __iter__(self) -> Iterator[TBatchResult]:
        return self

    def __next__(self) -> TBatchResult:
        if not self.id_to_params and not self.id_to_results:
            raise StopIteration()
        while self.preserve_order and self.next_id_in_order not in self.id_to_results \
                or not self.id_to_results:
            with self.cv:
                self.cv.wait(timeout=0.1)
            if self.stopped:
                raise StopIteration()

        if self.preserve_order:
            batch_id = self.next_id_in_order
            self.next_id_in_order += 1
        else:
            batch_id = next(iter(self.id_to_results.keys()))
        res = self.id_to_results[batch_id]
        del self.id_to_results[batch_id]
        self.sync_workers()
        return res.result

    def stop(self):
        self.stopped = True


