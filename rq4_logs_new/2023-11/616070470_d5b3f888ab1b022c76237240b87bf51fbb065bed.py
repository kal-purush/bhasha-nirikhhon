from datetime import timedelta
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingInput, TestingOutput, run_main


def test_batch_operator():
    in_data = [("ALL", x) for x in range(10)]
    flow = Dataflow()
    flow.input("in", TestingInput(in_data))
    # Use a long timeout to avoid triggering that.
    # We can't easily test system time based behavior.
    flow.batch("batch", max_size=3, timeout=timedelta(seconds=10))
    out = []
    flow.output("out", TestingOutput(out))
    run_main(flow)
    assert sorted(out) == sorted(
        [
            ("ALL", [0, 1, 2]),
            ("ALL", [3, 4, 5]),
            ("ALL", [6, 7, 8]),
            ("ALL", [9]),
        ]
    )