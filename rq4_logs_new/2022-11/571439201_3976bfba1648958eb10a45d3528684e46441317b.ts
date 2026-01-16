import abi from "../artifacts/contracts/TokenizedBallot.sol/Ballot.json";
import { ethers } from "hardhat";

const CONTRACT_ADDRESS = "0x19dD5214e03849266bf498738Edc4285298D9d0f";
const contractABI = abi.abi;
const PROPOSALS_COUNT = 3;

async function main() {
    const provider = ethers.getDefaultProvider("goerli");
    const contract = new ethers.Contract(CONTRACT_ADDRESS, contractABI, provider);

    console.log("\n---------RESULTS---------\n");
    for(let i = 0; i<PROPOSALS_COUNT; i++){
        const result = await contract.proposals(i);
        const name = ethers.utils.parseBytes32String(result.name);
        const votes = result.voteCount;
        console.log(`${name} -------> ${votes}`);
        if(i == PROPOSALS_COUNT-1){
            process.exit();
        }
    }
}

main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
});