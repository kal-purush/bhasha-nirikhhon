import argparse
import gc
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from tqdm import tqdm

from datasets import DatasetDict, load_dataset

from lang_def import WIKIPEDIA_LANGUAGES

_MAX_SHARD_SIZE = 256 * 1024 * 1024


def main(
    languages: list[str],
    date: str,
    dest_dirpath: Path,
    cache_dirpath: Optional[Path],
    mirror_url: str,
    override: bool = False,
):
    for language in languages:
        print("=====================")
        print(f"start processing `{language}`")
        start_time = datetime.now()

        lang_dest_dirpath = dest_dirpath / language
        if lang_dest_dirpath.exists():
            if override:
                print(f"overriding `{language}` at {lang_dest_dirpath}")
                shutil.rmtree(lang_dest_dirpath)
            else:
                print(
                    f"skipping `{language}` because {lang_dest_dirpath} already exists"
                )
                print(f"done  processing `{language}` ✅")
                print("=====================\n\n")
                continue

        build_kwargs = {
            "language": language.replace("-", "_"),
            "date": date,
            "mirror_url": mirror_url,
            "beam_runner": "DirectRunner",
            # "beam_runner": "apache_beam.runners.dask.dask_runner.DaskRunner",
        }
        if cache_dirpath:
            build_kwargs["cache_dir"] = str(cache_dirpath)

        try:
            dsd = load_dataset(
                "./prep/ds_script.py",
                **build_kwargs,
            )
            elapsed = datetime.now() - start_time
            assert isinstance(dsd, DatasetDict)
            print(f"done  processing `{language}` (elapsed: {elapsed})")

            for split_name, split in dsd.items():
                # With a beam runner, the dataset_size property is not actually
                # the number of bytes.

                dataset_size = split.download_size
                assert isinstance(dataset_size, int)
                num_shards = 1 + dataset_size // _MAX_SHARD_SIZE
                print(f"start splitting `{language}` into {num_shards} shards")

                shard_filenames = []
                for shard_index in range(num_shards):
                    ds_shard = split.shard(num_shards, shard_index)

                    shard_suffix = f"{shard_index+1:04d}-of-{num_shards:04d}"
                    dest_filename = f"{split_name}-{shard_suffix}.parquet"
                    shard_filenames.append(dest_filename)

                    os.makedirs(lang_dest_dirpath, exist_ok=True)
                    ds_shard.to_parquet(lang_dest_dirpath / dest_filename)

                # Create the info.json file
                info = {"shards": shard_filenames}
                with open(lang_dest_dirpath / "info.json", "w") as f:
                    json.dump(info, f)

            del dsd

        except Exception as e:
            print("❌❌❌❌")
            print(f"error with {language}: {e}")
            print("❌❌❌❌")

        gc.collect()
        print(f"done  processing `{language}` ✅")
        print("=====================\n\n")


if __name__ == "__main__":

    def formatter(prog):
        return argparse.HelpFormatter(prog, max_help_position=52)

    parser = argparse.ArgumentParser(
        prog="Wikipedia Builder",
        description="Prepares the Wikipedia dataset for each language",
        formatter_class=formatter,
    )
    parser.add_argument(
        "--date",
        help="Wikipedia dump date (e.g. 20230601)",
        default="20230601",
        type=str,
    )
    parser.add_argument(
        "--language",
        help="Language code (e.g. en). Default to processing all languages",
        type=str,
        nargs="*",
        metavar="LANG",
    )
    parser.add_argument(
        "--cache-dir",
        help="Cache directory for 🤗 Datasets",
        default=None,
        metavar="DIR",
        type=str,
    )
    parser.add_argument(
        "--mirror-url",
        help="Mirror URL",
        default="https://dumps.wikimedia.org",
        type=str,
    )
    parser.add_argument(
        "--override", help="Override existing files", action="store_true"
    )
    args = parser.parse_args()

    # Prepare the arguments
    if not args.language:
        languages = WIKIPEDIA_LANGUAGES[args.date]
    else:
        languages = args.language
    print("languages to process:", languages)
    dest_dirpath = Path("./data") / args.date

    main(
        languages,
        args.date,
        dest_dirpath,
        cache_dirpath=args.cache_dir,
        mirror_url=args.mirror_url,
        override=args.override,
    )