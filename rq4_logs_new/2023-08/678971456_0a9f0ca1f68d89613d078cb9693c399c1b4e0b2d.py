#! /usr/bin/env python

import click
import json
from datetime import datetime
from itertools import compress, chain, islice

def chunk(seq, size):
    seq = iter(seq)
    return iter(lambda: tuple(islice(seq, size)), ())

def compact(seq, size, sep = ","):
    return [sep.join(a) for a in chunk(seq, size)]

@click.command()
@click.option("--name", help = "repo name", required = True)
@click.option("--old", help = "old JSON", required = True)
@click.option("--patch", help = "patch JSON", required = True)
@click.option("--output", help = "output JSON", required = True)
@click.option("--chunk-size", help = "chunk-size", default = 30)
def updater (name, old, patch, output, chunk_size):
    """
    update the `name` item in the `old` JSON with the `patch` JSON,
    and save the updated JSON to `output`
    """
    today = datetime.today()
    try:
        with open(old, "r") as FH:
            d = json.load(FH)
    except Exception:
        d = dict()
    try:
        with open(patch, "r") as FH:
            d_patch = json.load(FH)
    except Exception:
        d_patch = {"clones": []}

    patch_dates = [
        datetime.fromisoformat(e["timestamp"] + "+00:00")
        for e in d_patch["clones"]
    ]
    if not patch_dates:
        with open(output, "w") as FH:
            json.dump(d, FH, indent = 2, sort_keys = True)
        return
    
    patch_counts = [e["count"] for e in d_patch["clones"]]
    patch_uniques = [e["uniques"] for e in d_patch["clones"]]
    if patch_dates[-1].date() == today.date(): # the stat could not be frozon yet
        patch_dates = patch_dates[:-1]
        patch_counts = patch_counts[:-1]
        patch_uniques = patch_uniques[:-1]
        d_patch["count"] = sum(patch_counts)
        d_patch["uniques"] = sum(patch_uniques)
        patch_counts = [str(i) for i in patch_counts]
        patch_uniques = [str(i) for i in patch_uniques]

    if not patch_dates:
        with open(output, "w") as FH:
            json.dump(d, FH, indent = 2, sort_keys = True)
        return
    
    if name in d:
        end = datetime.fromisoformat(d[name]["end"])
        if patch_dates[-1] > end: # has update
            delta = chain.from_iterable(s.split(",") for s in d[name]["date_delta"])
            counts = chain.from_iterable(s.split(",") for s in d[name]["counts"])
            uniques = chain.from_iterable(s.split(",") for s in d[name]["uniques"])
            new_counts = compress(patch_counts, [ts > end for ts in patch_dates])
            new_uniques = compress(patch_uniques, [ts > end for ts in patch_dates])
            patch_dates = [end, *[ts for ts in patch_dates if ts > end]]
            date_deltas = [
                str((patch_dates[i] - patch_dates[i-1]).days)
                for i in range(1, len(patch_dates))
            ]
            d[name]["end"] = patch_dates[-1].isoformat()
            d[name]["count"] = d[name]["count"] + d_patch["count"]
            d[name]["unique"] = d[name]["unique"] + d_patch["uniques"]
            d[name]["date_delta"] = compact([*delta, *date_deltas], chunk_size)
            d[name]["counts"] = compact([*counts, *new_counts], chunk_size)
            d[name]["uniques"] = compact([*uniques, *new_uniques], chunk_size)
    else:
        d[name] = {
            "start": patch_dates[0].isoformat(),
            "end": patch_dates[-1].isoformat(),
            "date_delta": compact([
                "0", *[
                    str((patch_dates[i] - patch_dates[i-1]).days)
                    for i in range(1, len(patch_dates))
                ]
            ], chunk_size),
            "count": d_patch["count"],
            "unique": d_patch["uniques"],
            "counts": compact(patch_counts, chunk_size),
            "uniques": compact(patch_uniques, chunk_size)
        }
    with open(output, "w") as FH:
        json.dump(d, FH, indent = 2, sort_keys = True)

if __name__ == "__main__":
    updater()