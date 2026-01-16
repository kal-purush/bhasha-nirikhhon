"""
Ethereum logs profile
"""

from locust import constant_pacing, task

from chainbench.user.evm import EVMBenchUser


class GetLogsProfile(EVMBenchUser):
    wait_time = constant_pacing(10)

    @task
    def get_logs_task(self):
        self.make_call(
            name="get_logs",
            method="eth_getLogs",
            params=self._get_logs_params_factory(),
        ),

    @task
    def get_block_by_number_task(self):
        self.make_call(
            name="get_block_by_number",
            method="eth_getBlockByNumber",
            params=self._block_by_number_params_factory(),
        ),
    
    @task
    def get_balance_task(self):
        self.make_call(
            name="get_balance",
            method="eth_getBalance",
            params=self._get_balance_params_factory_latest(),
        ),

    @task
    def chain_id_task(self):
        self.make_call(
            name="chain_id",
            method="eth_chainId",
            params=[],
        ),