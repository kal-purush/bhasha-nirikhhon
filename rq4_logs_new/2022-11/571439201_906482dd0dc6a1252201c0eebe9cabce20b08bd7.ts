import abi from "../artifacts/contracts/TokenizedBallot.sol/Ballot.json";
import { ethers } from "hardhat";
import prompt from "prompt-sync";

const CONTRACT_ADDRESS = "";
const contractABI = abi.abi;

const showPrompt = prompt();

async function main() {
  const provider = ethers.getDefaultProvider("goerli");

  const contract = new ethers.Contract(CONTRACT_ADDRESS, contractABI, provider);

  const voter = showPrompt("Please your wallet address: ");
  const proposal = showPrompt(
    "Please enter the index of the proposal you want to vote for: "
  );
  const amount = showPrompt("Please enter the amount of votes: ");

  try {
    const voteTx = await contract.connect(voter).vote(proposal, amount);
    console.log("Awaiting transaction confirmation....");
    const receipt = await voteTx.wait();
    console.log(
      `Your vote of ${amount} to Proposal #${proposal} is successful`
    );
    console.log(receipt);
  } catch (err) {
    console.log(err);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});