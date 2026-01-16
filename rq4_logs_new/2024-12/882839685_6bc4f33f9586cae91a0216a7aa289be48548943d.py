import json
import sys

import solcx
from web3 import Web3, HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

from errors import SolidityCompilationError, SolidityDeploymentError

_solc_version = "0.8.17"
solcx.install_solc(_solc_version)
solcx.set_solc_version(_solc_version)


class SolidityDeployer:
    """
    Deploys a Solidity contract to the blockchain. generating the ABI and bytecode
    """

    def __init__(self, blockchain_address: str, gas_limit: int):
        self.web3 = Web3(HTTPProvider(blockchain_address))
        self.web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self.gas_limit = gas_limit
        self.account_private_key = None

    def load_account(self, account_pk: str):
        """
        Loads an account to the Web3 instance.
        :param account_pk: str - the private key of the account
        """
        self.web3.eth.default_account = self.web3.eth.account.from_key(account_pk)
        self.account_private_key = account_pk

    def compile_contract(self, contract_path: str, dump_compiled=False) -> dict:
        """
        Compiles a Solidity contract to generate the ABI and bytecode.
        Also, can save the contract's ABI and bytecode to the same directory as the contract.
        :param contract_path: str - the path to the Solidity contract
        :param dump_compiled: bool - whether to save the compiled contract to a file
        :raises FileNotFoundError: if the contract file is not found
        :raises SolidityCompilationError: if the contract compilation fails
        :return: dict - the compiled contract containing the ABI and bytecode
        """

        try:
            with open(contract_path, 'r') as file:
                contract_code = file.read()
                compiled_contract = solcx.compile_source(contract_code, output_values=['abi', 'bin'])
                if dump_compiled:
                    contract_raw_name = list(compiled_contract.keys())[0]
                    contract_name = list(compiled_contract.keys())[0].split(':')[-1]
                    contract_id, contract_interface = compiled_contract.popitem()

                    abi = contract_interface['abi']
                    # dump json abi
                    abi = json.dumps(abi)
                    with open(f'{contract_path.split(".")[0]}.abi', 'w') as abi_file:
                        abi_file.write(abi)
                    bytecode = contract_interface['bin']
                    with open(f'{contract_path.split(".")[0]}.bin', 'w') as bytecode_file:
                        bytecode_file.write(bytecode)
                return compiled_contract
        except FileNotFoundError:
            raise FileNotFoundError(f'Contract file {contract_path} not found')
        except Exception as e:
            raise SolidityCompilationError(f'Error compiling contract {contract_path}: {e}')

    def deploy_contract(self, contract_path: str, *args):
        """
        Deploys a Solidity contract to the blockchain.
        :param contract_path: str - the path to the Solidity contract
        :param args: the arguments to pass to the contract constructor
        :return: str - the contract address
        :raises FileNotFoundError: if the contract file is not found
        :raises SolidityCompilationError: if the contract compilation fails
        """

        try:
            with open(f'{contract_path.split(".")[0]}.abi', 'r') as abi_file:
                abi = abi_file.read()
            with open(f'{contract_path.split(".")[0]}.bin', 'r') as bytecode_file:
                bytecode = bytecode_file.read()
            contract = self.web3.eth.contract(abi=abi, bytecode=bytecode)
            # send transaction that deploys the contract signed with the account private key
            tx = contract.constructor(*args).build_transaction({'from': self.web3.eth.default_account.address,
                                                                'gasPrice': self.web3.eth.gas_price,
                                                                'gas': self.gas_limit,
                                                                'nonce': self.web3.eth.get_transaction_count(self.web3.eth.default_account.address)})
            tx_hash = self.web3.eth.send_raw_transaction(
                self.web3.eth.account.sign_transaction(tx, self.account_private_key).raw_transaction)
            # wait for the transaction to be mined
            tx_receipt = self.web3.eth.wait_for_transaction_receipt(tx_hash)
            return tx_receipt['contractAddress']
        except FileNotFoundError:
            raise FileNotFoundError(f'Contract file {contract_path} not found')
        except Exception as e:
            raise SolidityDeploymentError(f'Error deploying contract {contract_path}: {e}')


if __name__ == "__main__":
    # get arguments
    user_pk = sys.argv[1]

    deployer = SolidityDeployer('http://localhost:8555', 4100000)
    deployer.load_account(user_pk)
    contract = deployer.compile_contract('contracts/KeycloakLogs.sol', dump_compiled=True)
    print("Contract compiled")
    contract_address = deployer.deploy_contract('contracts/KeycloakLogs.sol')
    print(f'Contract deployed at address: {contract_address}')