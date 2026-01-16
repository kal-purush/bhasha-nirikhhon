import { BigNumber } from "ethers";

export { };
const hre = require("hardhat");

const ethers = hre.ethers;

const network = hre.network.name;


const {
    DARK_BOARD_ADDRESS,
    CHEST_ADDRESS
} = require('./utils/main_params');

async function main() {

    const partner_address = "0x0dE5199779b43E13B3Bec21e91117E18736BC1A8"
    const partner_receiver_address = "0xF930EBBd05eF8b25B1797b9b2109DDC9B0d43063"
    const partenr_share = BigNumber.from('2500')

    const deployer = (await hre.ethers.getSigners())[0];

    const DarkQuestPartner = await ethers.getContractFactory("DarkQuestPartner");
    console.log('Deploying DarkQuestPartner  ...')

    const partner = await DarkQuestPartner.deploy(
        DARK_BOARD_ADDRESS,
        CHEST_ADDRESS,
        partner_address,
        partner_receiver_address,
        partenr_share
    )
    await partner.deployed()

    console.log('DarkQuestPartner : ', partner.address)

    await partner.deployTransaction.wait(15);


    await hre.run("verify:verify", {
        address: partner.address,
        constructorArguments: [
            DARK_BOARD_ADDRESS,
            CHEST_ADDRESS,
            partner_address,
            partner_receiver_address,
            partenr_share
        ],
    });

    console.log('To Do: Give approval from QuestChest to the DarkQuestPartner contract')
}

main()
    .then(() => process.exit(0))
    .catch(error => {
        console.error(error);
        process.exit(1);
    });