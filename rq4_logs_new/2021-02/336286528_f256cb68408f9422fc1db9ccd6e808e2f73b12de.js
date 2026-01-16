const Web3 = require('web3');           //for web3 calls
const Tx = require('ethereumjs-tx');    //for Tx calls

 //this will directly communicate with ropsten
 // Previous video check how to get this api " Vr1GWcLG0XzcdrZHWMPu "
const web3 = new Web3(new Web3.providers.HttpProvider("http://127.0.0.1:7545"));

const account = "0xc0AD6C154aaC2187AAB0F27065e568ec78c3909F" // account address

// private key
const privateKey = Buffer.from('c450ad261d7ec9281f308ddbe0abda0b324bc0039194db099320bd05b80ff5d9', 'hex');
const contractAddress = '0x1A3d4ed4F9311F192f7D8F6a73ca37CDf969DA6B'; // Deployed manually
const abi = require('./contracts/wallet').abi;

// interface for contract
const contract = new web3.eth.Contract(abi, contractAddress, {
  from: account,
  gasLimit: 3000000,
});