import { ethers } from "hardhat";
import { TestERC20__factory, TestERC20, ISchemaRegistry__factory, ISchemaRegistry } from "../typechain-types"


async function main() {
  
  const [ owner ] = await ethers.getSigners();

  const testERC20 = await ethers.getContractAt('TestERC20', "0xd55c3f5961Ec1ff0eC1741eDa7bc2f5962c3c454")
  await testERC20.mint(ethers.parseEther("1000"),"0x873006C1A901CA82859acF806937B0dBA3d61952")

  console.log(
    `TestERC20: ${ testERC20.target}`
  );
}

// We recommend this pattern to be able to use async/await everywhere
// and properly handle errors.
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});