require("@nomiclabs/hardhat-waffle");

const INFURA_PROJECT_ID = "<YOUR_INFURA_PROJECT_ID>";
const PRIVATE_KEY = "35a2d507e8c1324e17122743bd66948045b50d85dac6147b07454274e6a4705c";

module.exports = {
  solidity: "0.8.9",
  networks: {
    sepolia: {
      url: `https://eth-sepolia.g.alchemy.com/v2/P6HmomtcEZfctoJsmxXKP0rU21bCTcvT`,
      accounts: [PRIVATE_KEY],
    },
  },
};