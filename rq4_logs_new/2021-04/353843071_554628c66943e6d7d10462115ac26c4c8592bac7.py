#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import gc
import logging
from multiprocessing import Manager, Pool
import os
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pytz
import s3fs

from s3access.normalize import deserialize_file
from s3access.parquet import write_dataset
from s3access.schema import create_schema


def parse_time(object_name):
    return datetime.strptime(object_name[0:19], "%Y-%m-%d-%H-%M-%S")


def create_files_index(src, hour, timezone, fs):
    """
    :param str src: The filesystem, s3 or local
    :param str hour: The hour being targetted in the format YYYY-MM-DD-HH
    :param str timezone: The timezone from pytz
    :param str fs:  The filesystem
    :return: A Data Frame with the set of files including path and datetime
    """

    files = []
    if src.startswith("s3://"):
        u = urlparse(src)
        files = [
            {"path": f, "dt": timezone.localize(parse_time(os.path.basename(f)))}
            for f in fs.glob(("{}{}/{}*").format(u.netloc, u.path, hour))
        ]
    else:
        files = [
            {
                "path": f.as_posix(),
                "dt": timezone.localize(parse_time(os.path.basename(f))),
            }
            for f in Path(src).rglob("*")
        ]

    return pd.DataFrame(files)


def create_file_system(root, endpoint_url, endpoint_region, s3_acl):
    if root.startswith("s3://"):
        return s3fs.S3FileSystem(
            anon=False,
            client_kwargs={
                "endpoint_url": endpoint_url,
                "region_name": endpoint_region,
            },
            s3_additional_kwargs={
                "ACL": s3_acl,
            },
        )
    return None


def aggregate_range(
    src,
    dst,
    files,
    timezone,
    logger,
    schema,
    input_file_system,
    outputFileSystem,
    cpu_count,
    timeout,
):

    items = []

    with Manager() as manager:  # noqa: F841
        logger.info("Deserializing data in files")

        results = []

        pool = Pool(processes=int(cpu_count / 2))

        for f in files.itertuples():
            result = pool.apply_async(
                deserialize_file,
                args=(f.path, input_file_system),
            )
            results += [result]

        pool.close()

        logger.info("Waiting for deserialization to complete")

        for i in range(len(results)):
            try:
                items.extend(results[i].get(timeout=timeout))
            except Exception as err:
                logger.info("Error deserializing", files[i]["path"], err)

        logger.info("Deserialization complete")

    if len(items) == 0:
        return

    gc.collect()

    df = pd.DataFrame(items)

    logger.info("Serializing {} items to {}".format(len(items), dst))

    items = []

    gc.collect()

    write_dataset(
        pa.Table.from_pandas(df, schema=schema, preserve_index=False),
        dst,
        compression="SNAPPY",
        partition_cols=["bucket_name", "operation", "year", "month", "day", "hour"],
        partition_filename_cb=lambda x: "-".join([str(y) for y in x]) + ".parquet",
        row_group_cols=["requester", "remoteip_int", "is_assumed_role", "is_user"],
        fs=outputFileSystem,
        cpu_count=cpu_count,
        makedirs=(not dst.startswith("s3://")),
        timeout=timeout,
    )

    logger.info(
        "Done serializing items to {}".format(
            dst,
        )
    )


def main():

    #
    # CPU Count
    #

    cpu_count = os.cpu_count()

    #
    # Now
    #

    utc = pytz.timezone("UTC")

    now = datetime.now(utc)

    #
    # Setup Logging
    #

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    #
    # Settings
    #

    src = os.getenv("SRC")
    dst = os.getenv("DST")
    hour = os.getenv("HOUR")

    input_s3_acl = os.getenv("INPUT_S3_ACL")
    input_s3_endpoint = os.getenv("INPUT_S3_ENDPOINT")
    input_s3_region = os.getenv("INPUT_S3_REGION")

    output_s3_acl = os.getenv("OUTPUT_S3_ACL")
    output_s3_endpoint = os.getenv("OUTPUT_S3_ENDPOINT")
    output_s3_region = os.getenv("OUTPUT_S3_REGION")

    timeout = int(os.getenv("TIMEOUT", "300"))

    logger.info("now: {}".format(now))
    logger.info("cpu_count: {}".format(cpu_count))
    logger.info("src: {}".format(src))
    logger.info("dst: {}".format(dst))
    logger.info("hour: {}".format(hour))
    logger.info("timeout: {}".format(timeout))

    if src is None or len(src) == 0:
        raise Exception("{} is missing".format("src"))

    if dst is None or len(dst) == 0:
        raise Exception("{} is missing".format("dst"))

    if hour is None or len(hour) == 0:
        raise Exception("{} is missing".format("hour"))

    if src[len(src) - 1] != "/":
        src = src + "/"

    #
    # Initialize File Systems
    #

    input_file_system = create_file_system(
        src, input_s3_endpoint, input_s3_region, input_s3_acl
    )
    output_file_system = create_file_system(
        dst, output_s3_endpoint, output_s3_region, output_s3_acl
    )

    #
    # Load Schema
    #

    schema = create_schema()

    all_files = create_files_index(
        src,
        hour,
        utc,
        input_file_system,
    )

    if len(all_files) == 0:
        raise Exception("no source files found within folder {}".format(src))

    logger.info(all_files)

    if not dst.startswith("s3://"):
        os.makedirs(dst, exist_ok=True)

    aggregate_range(
        src,
        dst,
        all_files,
        utc,
        logger,
        schema,
        input_file_system,
        output_file_system,
        cpu_count,
        timeout,
    )

    return None


if __name__ == "__main__":
    main()