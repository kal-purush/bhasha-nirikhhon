'''
Internal objects and methods.
'''

import functools
from typing import Callable, TypeVar
from typeguard import typechecked


T = TypeVar('T')


@typechecked
def run_with_specified_config(namespace: str) -> \
        Callable[[Callable[[T, object], T]], Callable[[T], T]]:
    '''
    Run a "config modification method" which depends on configuration in a
    specific "path".

        Parameters:
            namespace: Python namespace corresponding to the field to obtain
                from a configuration object. If `namespace` is 'a.b', then
                `inner_config` will be `config['a']['b']`.
    '''

    def decorator(func):

        @functools.wraps(func)
        def inner_func(config):
            inner_config = config

            try:
                for name in namespace.split('.'):
                    inner_config = inner_config[name]
            except KeyError:
                return config

            return func(config, inner_config)

        return inner_func

    return decorator