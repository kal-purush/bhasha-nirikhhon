import { ethers } from "hardhat";

async function main() {
  const [deployer] = await ethers.getSigners();

  const XGameCardFactory = await ethers.getContractFactory("XGameCard");
  const XGameContract = await XGameCardFactory.attach(
    "0x29194B2189b6Bdb67337E70323cE22F68c1E2c7e"
  );
  await XGameContract.deployed();

  const mintNFT = await XGameContract.connect(deployer).safeMint(
    3131,
    deployer.address
  );
  await mintNFT.wait();

  let contractAddresses = new Map<string, string>();
  contractAddresses.set("NFT CONTRACT => ", XGameContract.address);
  contractAddresses.set("DEPLOYER     => ", deployer.address);
  console.table(contractAddresses);
}

/*
npx hardhat run scripts/game/nftAttach.ts --network nebula-testnet
*/

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});