"""Module providing set of Ecommerce API functions."""
import logging

from adobe_captivate_prime_api import CaptivatePrimeAPI


def get_ecommerce_max_price(
    api: CaptivatePrimeAPI,
    lo_types: str | list = "course",
) -> list:
    """Retrieves max price of any LO present in account.

    :param api: CaptivatePrimeAPI
    :param lo_types: str or list
    :return: list

    """

    lo_types_options = [
        "course",
        "learningProgram",
        "certification",
    ]

    if isinstance(lo_types, str):
        if lo_types not in lo_types_options:
            lo_types = "course"
            logging.debug(
                'Invalid lo_types value, "course" used as default. Expected values: %s',
                lo_types_options,
            )
    elif isinstance(lo_types, list):  # pylint:disable=confusing-consecutive-elif
        for lo_type in lo_types:
            if lo_type not in lo_types_options:
                lo_types.remove(lo_type)
                logging.debug(
                    "Invalid lo_type value(s) are removed, and used as %s. "
                    "Expected values: %s",
                    lo_types,
                    lo_types_options,
                )

    params = {
        "filter.loTypes": lo_types,
    }

    return api.fetch(
        method="GET",
        endpoint="ecommerce/maxPrice",
        params=params,
    )