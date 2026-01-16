# coding=utf-8
import sys
sys.path.append('/scratch/pradap/python-work/dmagellan/dmagellan')
from dask import threaded
from dmagellan.blocker.overlap import overlapblocker

from runtime import utils


def profile_candset(candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                    r_key, l_block_attr, r_block_attr,
                    rem_stop_words=False, q_val=None, word_level=True,
                    overlap_size=1,
                    scheduler=threaded.get,
                    num_workers=None, cache_size=1e9, compute=False,
                    show_progress=True, grid_params={}
                    ):
    # call the profiler command with different sample proportions
    d = get_args_dict(ltable, rtable, fk_ltable, fk_rtable, l_key,
                      r_key, l_block_attr, r_block_attr,
                      rem_stop_words, q_val, word_level,
                      overlap_size,
                      scheduler,
                      num_workers, cache_size, compute)
    ab = overlapblocker.OverlapBlocker()

    estimated_times = []
    # pbar = ProgBar(len(grid_params['nchunks']))
    
    for chunk_size in grid_params['nchunks']:
        print(chunk_size)
        # pbar.update()
        d['nchunks'] = chunk_size
        proc = (ab.block_candset, (candset), d)
        estimated_time, _, _ = utils.profiler_candset(proc, repeat=1)
        estimated_times.append((chunk_size, estimated_time))
    return estimated_times


def get_args_dict(ltable, rtable, fk_ltable, fk_rtable, l_key,
                  r_key, l_block_attr, r_block_attr,
                  rem_stop_words, q_val, word_level,
                  overlap_size,
                  scheduler, num_workers, cache_size, compute):
    d = {}
    d['ltable'] = ltable
    d['rtable'] = rtable
    d['fk_ltable'] = fk_ltable
    d['fk_rtable'] = fk_rtable
    d['l_key'] = l_key
    d['r_key'] = r_key
    d['l_block_attr'] = l_block_attr
    d['r_block_attr'] = r_block_attr
    d['rem_stop_words'] = rem_stop_words
    d['q_val'] = q_val
    d['word_level'] = word_level
    d['overlap_size'] = overlap_size
    d['scheduler'] = scheduler
    d['num_workers'] = num_workers
    d['cache_size'] = cache_size
    d['compute'] = compute
    d['show_progress'] = False
    d['nchunks'] = 1

    return d



    
                
    