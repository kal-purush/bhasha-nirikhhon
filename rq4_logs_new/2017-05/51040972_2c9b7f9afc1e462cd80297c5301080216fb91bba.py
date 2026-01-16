"""A hook for modifying parameter values read from the WMT client."""

import os
import shutil

from wmt.utils.hook import find_simulation_input_file, yaml_dump


def execute(env):
    """Perform pre-stage tasks for running a component.

    Parameters
    ----------
    env : dict
      A dict of component parameter values from WMT.

    """
    env['analysis_driver'] = 'dakota_run_component'

    for fname in file_list:
        src = find_simulation_input_file(env[fname])
        shutil.copy(src, os.curdir)

    yaml_dump('_env.yaml', env)