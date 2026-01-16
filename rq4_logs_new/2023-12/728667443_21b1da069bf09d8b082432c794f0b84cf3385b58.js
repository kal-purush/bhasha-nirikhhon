const hre = require("hardhat");

async function main() {
  const messageSigningContract = await hre.ethers.deployContract(
    " MessageSigning"
  );

  await messageSigningContract.waitForDeployment();

  console.log(
    ` messageSigningContract deployed to ${messageSigningContract.target}`
  );
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});