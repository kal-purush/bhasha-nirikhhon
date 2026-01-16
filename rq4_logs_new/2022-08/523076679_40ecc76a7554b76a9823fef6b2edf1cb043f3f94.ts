import hre, { ethers } from "hardhat";
import addressUtils from "../../utils/addressUtils";
import { OmniCounter__factory } from "../../typechain-types";

export async function deployOmniCounterMumbai() {
  const ethEndpoint = "0xf69186dfBa60DdB133E91E9A4B5673624293d8F8";

  const OmniCounterMumbai = (await ethers.getContractFactory(
    "OmniCounter"
  )) as OmniCounter__factory;

  const OmniCounter = await OmniCounterMumbai.deploy(ethEndpoint);
  await OmniCounter.deployed();

  console.log("Deployed OmniCounterMumbai to: ", OmniCounter.address);

  await addressUtils.saveAddresses(hre.network.name, {
    omniCounter: OmniCounter.address,
  });
}

deployOmniCounterMumbai().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});