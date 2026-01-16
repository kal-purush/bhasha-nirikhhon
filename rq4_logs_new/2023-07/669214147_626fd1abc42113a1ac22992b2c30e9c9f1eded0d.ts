import { HardhatUserConfig } from "hardhat/config";
import "@nomicfoundation/hardhat-toolbox";
import '@typechain/hardhat'
import * as dotenv from 'dotenv'
dotenv.config()


const config: HardhatUserConfig = {
  solidity: {
    compilers: [
      {
        version: "0.8.18",
        settings: {
          optimizer: {
            enabled: true,
            runs: 100,
          },
        },
      },
    ],
  },
  networks: {
    mainnet:{
      url: process.env.ALCHEMY_MAINNET_URL!,
      accounts: [process.env.DEPLOYER_KEY!]
    },
    sepolia: {
      url: process.env.ALCHEMY_SEPOLIA_URL!,
      accounts: [process.env.DEPLOYER_KEY!]
    },
    hardhat: {
      forking: {
        url: process.env.ALCHEMY_SEPOLIA_URL!,
      },
      allowUnlimitedContractSize: true,
    }
  },
  etherscan: {
    apiKey: {
      sepolia: process.env.ETHERSCAN_API_KEY!,
      mainnet: process.env.ETHERSCAN_API_KEY!
    }
  },
  typechain: {
    outDir: 'typechain-types',
    target: 'ethers-v6',
    externalArtifacts: [
    ], 
  },
};

export default config;
