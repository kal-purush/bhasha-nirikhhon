#!/usr/bin/env python
# -*- coding: utf-8 -*-

from parsl.config import Config
from parsl.channels import LocalChannel
from parsl.executors import HighThroughputExecutor
from parsl.providers import TorqueProvider
from parsl.addresses import address_by_route, address_by_query, address_by_hostname
from parsl.launchers import SingleNodeLauncher

sbayesr_config = Config(
    executors=[
        HighThroughputExecutor(
            label="gardner_plink_1ppn",
            cores_per_worker=1,
            #  address=address_by_hostname(), # Gets cri16in001 as address
            #  address=address_by_query(), # Gets 128.135.245.36 as address
            address=address_by_route(), # Gets 10.50.178.250 as address
            # address="128.135.245.36",
            provider=TorqueProvider(
                channel=LocalChannel(), # Default if not given
                # launcher=SingleNodeLauncher(),
                worker_init="module load gcc/6.2.0; module load plink/2.0; module load python/3.8.1; python3 -c 'import parsl; print(parsl.__version__)'",
                walltime="01:00:00",
                scheduler_options="#PBS -l mem=2gb",
                nodes_per_block=1,
                max_blocks=1
            ),
        )
    ],
)