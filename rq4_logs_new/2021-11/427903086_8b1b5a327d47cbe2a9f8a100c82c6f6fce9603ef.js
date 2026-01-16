function deploy(from) {
	Contract.deploy({data : byteCode, arguments:[]}).send({data: byteCode, from: from, gas: 470000});
}

const contractName = 'Group';
const fileSol = contractName+'.sol';

Web3 = require('web3');
fs = require("fs");
web3 = new Web3(new Web3.providers.HttpProvider("http://localhost:8545"));
code = fs.readFileSync(fileSol, 'utf8');
solc = require('solc');
var solcInput = {
    language: "Solidity",
    sources: { 
        contract: {
            content: code
        }
     },
    settings: {
        optimizer: {
            enabled: true
        },
        evmVersion: "byzantium",
        outputSelection: {
            "*": {
              "": [
                "legacyAST",
                "ast"
              ],
              "*": [
                "abi",
                "evm.bytecode.object",
                "evm.bytecode.sourceMap",
                "evm.deployedBytecode.object",
                "evm.deployedBytecode.sourceMap",
                "evm.gasEstimates"
              ]
            },
        }
    }
};

solcInput = JSON.stringify(solcInput);
contractObject = solc.compile(solcInput);
abiDefinition = JSON.parse(contractObject);
Contract = new web3.eth.Contract(abiDefinition.contracts.contract[contractName].abi);
console.log('abi = ' +JSON.stringify(abiDefinition.contracts.contract[contractName].abi));
byteCode = abiDefinition.contracts.contract[contractName].evm.bytecode.object;
web3.eth.getAccounts().then((accounts) => {console.log(accounts);deploy(accounts[0]);});