import { ethers } from "hardhat";
import * as dotenv from "dotenv";

import prompt from "prompt-sync";

import abi from "../artifacts/contracts/TokenizedBallot.sol/Ballot.json";

dotenv.config();

const CONTRACT_ADDRESS = "";
const contractABI = abi.abi;

const showPrompt = prompt();

async function main() {
  const provider = ethers.getDefaultProvider("goerli");

  const contract = new ethers.Contract(CONTRACT_ADDRESS, contractABI, provider);

  const voter = showPrompt("Please your wallet address: ");

  let votePower = await contract.getVotes(voter);
  console.log(`The voter ${voter} has voting power of ${votePower}`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});