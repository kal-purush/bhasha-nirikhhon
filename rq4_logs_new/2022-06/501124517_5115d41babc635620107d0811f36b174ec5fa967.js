const hre = require("hardhat");
const loadJsonFile = require("load-json-file");

const {ethers} = hre;
const { parseEther } = ethers.utils;
const PCS_ROUTER = "0x10ED43C718714eb63d5aA57B78B54704E256024E";
const GEM = "0x701F1ed50Aa5e784B8Fb89d1Ba05cCCd627839a7";

async function main() {

  const GemBurner = await ethers.getContractFactory("GemBurner");
  const gemBurner = await GemBurner.deploy(
        GEM,//ERC20PresetFixedSupply _gemToken,
        parseEther("100"),//uint256 _burnTriggerWad,
        PCS_ROUTER,//IAmmRouter02 _router,
        "0x70e1cB759996a1527eD1801B169621C18a9f38F9"//address _admi
        );
  await gemBurner.deployed();
  console.log("gemBurner deployed to:", gemBurner.address);
}

// We recommend this pattern to be able to use async/await everywhere
// and properly handle errors.
main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error);
    process.exit(1);
  });