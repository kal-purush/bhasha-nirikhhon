// scripts/upgrade_box_v2.js

const { ethers, upgrades } = require("hardhat");

const PROXY = "0x719479Aa893348B75fE1D99F85cb5B22A7aa9f18"; // 프록시 컨트랙트

async function main() {
    const AirDropV2 = await ethers.getContractFactory("AirDropV2");
    console.log("Upgrading AirDrop...");
    await upgrades.upgradeProxy(PROXY, AirDropV2);
    console.log("AirDropV2 upgraded");
}

main();

