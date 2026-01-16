const deploy = async () => {
  const { deployer } = await ethers.getSigners();
  console.log("Desplegando el contracto con la cuenta:".deployer.address);
  const GalleryNFT = await ethers.getContractFactory("GalleryNFT");
  const deployed = await GalleryNFT.deploy();
  console.log("GalleryNFT fue desplegado en: ", deployed.address);
};

deploy()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error();
    process.exit(1);
  });