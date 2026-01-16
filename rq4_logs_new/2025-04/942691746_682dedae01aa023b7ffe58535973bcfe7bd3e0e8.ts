import { HardhatUserConfig } from "hardhat/config";
import "@nomicfoundation/hardhat-toolbox";

// Đảm bảo bạn đã tạo file .env và thêm các biến sau:
// INFURA_API_KEY=xxxxxxx
// SEPOLIA_PRIVATE_KEY=xxxxxxx

import * as dotenv from "dotenv";
dotenv.config();

const INFURA_API_KEY = process.env.INFURA_API_KEY || "";
const SEPOLIA_PRIVATE_KEY = process.env.SEPOLIA_PRIVATE_KEY || "";

const config: HardhatUserConfig = {
  solidity: "0.8.28",
  etherscan: {
    apiKey: "TI5SA79CG77FB4T6KPCZ3QZFVU2NIUNPMC",  //tạo apikey ở stherscan web
  },
  networks: {
    sepolia: {
      url: `https://sepolia.infura.io/v3/${INFURA_API_KEY}`,
      accounts: [SEPOLIA_PRIVATE_KEY],
    },
  },
};

export default config;