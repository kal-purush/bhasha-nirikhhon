from typing import Tuple

from charmhub_lp_tools.constants import (
    LIST_OF_RISKS,
    Risks,
)


def parse_channel(value: str) -> Tuple[str, str]:
    """Parse a string that represents a channel.

    :param value: a string that represents a channel.
    :returns: a tuple with track and risk.
    """
    if value in LIST_OF_RISKS:
        # this is a risk-only value, we assume 'latest' track
        return ('latest', value)

    try:
        # track/risk
        (track, risk) = value.split('/')
        return (track, risk)
    except ValueError:
        pass

    if value not in LIST_OF_RISKS:
        # it's a track-only value, so we assume 'stable' risk.
        return (value, Risks.STABLE.value)

    raise ValueError('Could not parse %s' % value)