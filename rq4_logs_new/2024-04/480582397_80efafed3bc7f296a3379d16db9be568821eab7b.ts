import { HardhatRuntimeEnvironment } from "hardhat/types";

const UpdateWhitelist = async (hre: HardhatRuntimeEnvironment): Promise<void> => {
  const {
    deployments: { deploy, execute },
    getNamedAccounts,
    getChainId,
  } = hre;

  const chainId = await getChainId();
  let testnet: boolean;
  if (chainId === "84532") {
    // Base Sepolia
    testnet = true;
  } else if (chainId === "8453") {
    // Base Mainnet
    testnet = false;
  } else {
    console.error("Invalid chain");
    return;
  }

  console.log("testnet: ", testnet);

  const { deployer } = await getNamedAccounts();

  const artwork = await hre.ethers.getContract("MettaArtwork");

  const updateWhitelistData = (
    await hre.ethers.getContractFactory("Artwork", deployer)
  ).interface.encodeFunctionData("updateWhitelist", [
    1714137950, // whitelistStartTime
    [
      "0x073cf670AD2cf0e048924aC2C787903E76A0f389",
      "0x107a5ed5258CC0C962c18A86A72cCbEac7Fc3769",
      "0x6fAC5Ca3a8A7F53385e87a1E7e683Dd7486afc3b",
      "0x95607E95b3e2cdc1E31a556536C24993f05Ac42e",
    ],
    [1, 1, 1, 1],
  ]);

  await execute(
    "ProjectRegistry",
    { log: true, from: deployer },
    "execute",
    [artwork.address],
    [0],
    [updateWhitelistData]
  );
};

export default UpdateWhitelist;