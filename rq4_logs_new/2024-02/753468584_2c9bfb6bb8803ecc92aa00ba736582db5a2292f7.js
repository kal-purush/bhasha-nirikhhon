require("@nomicfoundation/hardhat-toolbox");
require("hardhat-flat-exporter")
require("dotenv").config();

const infura_api_key = process.env.THIRD_PARTY_API_KEY;
const private_key = process.env.PRIVATE_KEY;
const mnemonic = process.env.MNEMONIC;
const etherscan_api_key = process.env.ETHERSCAN_API_KEY;

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
  networks: {
    hardhat:{
    },
    sepolia: {
      url: infura_api_key,
      accounts: [private_key],
      chainId: 11155111,
      mnemonic: mnemonic,
    }
  },
  flattenExporter: {
    src: "./contracts",
    path: "./flat",
    clear: true,
  },
  etherscan: {
    apiKey: etherscan_api_key,
  },

  solidity: "0.8.19",
};