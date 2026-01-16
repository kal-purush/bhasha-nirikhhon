import logging
from time import time
import asyncio

import app.config as app_config
from app.libs.utils import chunkify
from app.model import (
    Submission,
    SubmissionResult,
    WorkPayload,
    BatchSubmission,
    BatchSubmissionResult,
    ResultReason,
)


logger = logging.getLogger(__name__)


def _to_result(submission: Submission, start_time, result_json):
    if result_json is None: # timeout
        return SubmissionResult(sub_id=submission.sub_id, success=False, cost=time() - start_time, reason=ResultReason.QUEUE_TIMEOUT)
    else:
        result = SubmissionResult.model_validate_json(result_json[1])
        if not result.success and result.cost >= app_config.MAX_EXECUTION_TIME:
            result.reason = ResultReason.WORKER_TIMEOUT
        return result


async def judge(redis_queue, submission: Submission):
    start_time = time()
    try:
        payload = WorkPayload(submission=submission)
        payload_json = payload.model_dump_json()
        await redis_queue.push(app_config.REDIS_WORK_QUEUE_NAME, payload_json)
        result_queue_name = f'{app_config.REDIS_RESULT_PREFIX}{payload.work_id}'
        result_json = await redis_queue.block_pop(result_queue_name, app_config.MAX_QUEUE_WAIT_TIME)
        await redis_queue.delete(result_queue_name)
        return _to_result(submission, start_time, result_json)
    except Exception:
        logger.exception(f'Failed to judge submission {submission.sub_id}')
        return SubmissionResult(sub_id=submission.sub_id, success=False, cost=time() - start_time, reason=ResultReason.INTERNAL_ERROR)


async def _judge_batch_impl(redis_queue, subs: list[Submission]):
    start_time = time()
    chunks = list(chunkify(subs, app_config.MAX_BATCH_CHUNK_SIZE))
    payload_chunks = [
        [WorkPayload(submission=sub) for sub in chunk]
        for chunk in chunks
    ]

    async def _submit(payload: WorkPayload):
        payload_json = payload.model_dump_json()
        await redis_queue.push(app_config.REDIS_WORK_QUEUE_NAME, payload_json)

    async def _get_result(payload: WorkPayload, max_wait_time):
        """max_wait_time <= 0 means no wait (which is different from block_pop)"""
        result_queue_name = f'{app_config.REDIS_RESULT_PREFIX}{payload.work_id}'
        if max_wait_time > 0:
            result_json = await redis_queue.block_pop(result_queue_name, max_wait_time)
        else:
            # no wait
            result_json = await redis_queue.pop(result_queue_name)
        await redis_queue.delete(result_queue_name)
        return _to_result(payload.submission, start_time, result_json)

    # submit all submissions to the queue
    for chunk in payload_chunks:
        await asyncio.gather(*[_submit(pl) for pl in chunk])

    results = []
    max_wait_time = app_config.MAX_QUEUE_WAIT_TIME
    wait_start_time = time()
    for chunk in payload_chunks:
        # get all results from the queue
        left_time = int(max_wait_time - (time() - wait_start_time))
        chunk_results = await asyncio.gather(*[_get_result(pl, left_time) for pl in chunk])
        results.extend(chunk_results)
    return results


async def judge_batch(redis_queue, batch_sub: BatchSubmission):
    try:
        if not app_config.MAX_BATCH_CHUNK_SIZE:
            return BatchSubmissionResult(
                sub_id=batch_sub.sub_id,
                results=await asyncio.gather(*[judge(redis_queue, sub) for sub in batch_sub.submissions])
            )
        results = await _judge_batch_impl(redis_queue, batch_sub.submissions)
    except Exception:
        logger.exception(f'Failed to judge batch submission {batch_sub.sub_id}')
        results=[
            SubmissionResult(
                sub_id=sub.sub_id,
                success=False,
                cost=0,
                reason=ResultReason.INTERNAL_ERROR
            ) for sub in batch_sub.submissions
        ]
    return BatchSubmissionResult(
        sub_id=batch_sub.sub_id,
        results=results
    )