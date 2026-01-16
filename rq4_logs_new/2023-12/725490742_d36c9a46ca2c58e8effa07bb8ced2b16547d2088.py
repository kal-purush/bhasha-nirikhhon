import argparse
import logging
import sys
from ast import literal_eval
from pathlib import Path

from utils.isocor2mtf import isocor2mtf
from utils.physiofit2mtf import physiofit2mtf


def _init_logger(log_path):
    """
    Initialize the root logger
    :return:
    """

    logger = logging.getLogger("root")
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(log_path, mode="w")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(levelname)s:%(name)s: %(message)s"
    )
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)


def args_parse():
    """
    Parse arguments from Command-Line Interface
    :return: Argument Parser containing args
    """

    parser = argparse.ArgumentParser(
        "Influx_si data manager: handling data interoperability on Workflow4Metabolomics, see here: "
        "workflow4metabolomics.usegalaxy.org"
    )

    # Get paths to data
    parser.add_argument(
        "-p", "--physiofit", type=str,
        help="Path to physiofit summary output file"
    )
    parser.add_argument(
        "-i", "--isocor", type=str,
        help="Path to isocor results output file"
    )
    parser.add_argument(
        "-m", "--misc", type=str,
        help="Path to collection containing paths to the extra files for influx_si (given as list)"
    )

    # Give output collection paths
    parser.add_argument(
        "-z", "--zip", type=str,
        help="Output path for zip containing all the files for influx launch"
    )
    parser.add_argument(
        "-l", "--log", type=str,
        help="Output path for log"
    )

    # # Add for galaxy launch
    # parser.add_argument(
    #     "-g", "--galaxy", action="store_true",
    #     help="Add argument for galaxy launch (to get path from args)"
    # )

    return parser


def process(args):
    # initialize root
    if hasattr(args, "log"):
        _init_logger(str(Path(args.log)))
    else:
        _init_logger("./log.txt")

    # get logger
    _logger = logging.getLogger("root")

    if hasattr(args, "galaxy"):
        mflux_dfs = physiofit2mtf(
            physiofit_res=args.physiofit
        )
        miso_dfs = isocor2mtf(
            isocor_res=args.isocor
        )
        list_of_miscs = args.misc
    else:
        mflux_dfs = physiofit2mtf(
            physiofit_res=r"C:\Users\legregam\PycharmProjects\influx_si_data_manager\test-data\physiofit2mtf"
                          r"\summary.csv",
        )

        miso_dfs = isocor2mtf(
            isocor_res=r"C:\Users\legregam\PycharmProjects\influx_si_data_manager\test-data\isocor2mtf"
                       r"\Data_example_res.tsv",
            sd=0.02
        )
        list_of_miscs = literal_eval(args.misc)

    mflux_names = [exp[0] for exp in mflux_dfs]
    miso_names = [exp[0] for exp in miso_dfs]

    if mflux_names != miso_names:
        msg = (f"Sample names in miso files and mflux files are not the same:\nmflux names: {mflux_names}"
               f"\nmiso names: {miso_names}")
        _logger.error(msg)
        raise ValueError(msg)

    mflux_path = Path("./mflux")
    if not mflux_path.exists():
        mflux_path.mkdir()
    miso_path = Path('./miso')
    if not miso_path.exists():
        miso_path.mkdir()
    for exp in mflux_dfs:
        exp[1].to_csv(str(mflux_path / f"{exp[0]}.tsv"), sep="\t")
    for exp in miso_dfs:
        exp[1].to_csv(str(miso_path / f"{exp[0]}.tsv"), sep="\t")


def main():
    parser = args_parse()
    args = parser.parse_args()
    process(args)