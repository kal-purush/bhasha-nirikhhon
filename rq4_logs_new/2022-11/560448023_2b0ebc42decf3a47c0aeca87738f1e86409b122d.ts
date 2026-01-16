import { HardhatUserConfig } from "hardhat/config";
import "@nomicfoundation/hardhat-toolbox";
import * as dotenv from "dotenv";
dotenv.config();

const AURORA_TESTNET_PRIVATE_KEY = process.env.AURORA_TESTNET_PRIVATE_KEY;
const AURORA_MAINNET_PRIVATE_KEY = process.env.AURORA_MAINNET_PRIVATE_KEY;
const AURORA_RPC_URL = process.env.AURORA_RPC_URL;
const AURORASCAN_API_KEY = process.env.AURORASCAN_API_KEY;

const config: HardhatUserConfig = {
  solidity: "0.8.17",
  networks: {
    aurora_testnet: {
      url: 'https://testnet.aurora.dev',
      accounts: [`0x${AURORA_TESTNET_PRIVATE_KEY}`],
      chainId: 1313161555
    },
    aurora_mainnet: {
      url: AURORA_RPC_URL,      
      accounts: [`0x${AURORA_MAINNET_PRIVATE_KEY}`],
      chainId: 1313161554,
    },
  },
  etherscan: {
    apiKey: AURORASCAN_API_KEY,
  },
};

export default config;