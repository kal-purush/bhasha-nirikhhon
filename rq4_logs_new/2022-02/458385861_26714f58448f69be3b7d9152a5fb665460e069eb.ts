import { getERC721Admin, getERC20Mojo } from "../misc/contract-hooks";
import { artifactMap } from "../misc/contract-hooks";
import fs from "fs";
import { ethers } from "hardhat";


async function main() {

  // Get contracts
  const network = await ethers.provider.getNetwork();
  const chainId = network.chainId;
  const erc721Admin = await getERC721Admin();
  const erc20Mojo = await getERC20Mojo();

  // Destruct
  if (erc721Admin && erc20Mojo) {
    // Destruct
    await erc721Admin.selfDestruct();
    await erc20Mojo.selfDestruct();
    /// @ts-ignore
    fs.rmSync(artifactMap[chainId], { recursive: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});