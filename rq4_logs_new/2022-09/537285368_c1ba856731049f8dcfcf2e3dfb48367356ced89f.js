require('dotenv').config();
const fs = require("fs").promises;
const {ethers, utils, Wallet} = require("ethers");
const {JsonRpcProvider} = require("@ethersproject/providers");
const {WEB3_API_URL} = require("./const");

const provider = new JsonRpcProvider(WEB3_API_URL.POLYGON_MUMBAI);
const BATCH_SIZE = 50
let totalAirdrop = 0
let totalUserAirdrop =0
const AIRDROP_ADDRESS = process.env.AIRDROP_CONTRACT_ADDRESS

const airdrop = async () => {
    const content = await fs.readFile("./data/test3.json")
    const data = JSON.parse(content);
    const total = data.reduce(function (currentAggregation, currentItem) {
        return  currentAggregation + currentItem.amount
    },0)
    console.log(`total airdrop: ${total}`);
    console.log(`total users: ${data.length}`);
    let wallet = new Wallet(process.env.DEPLOY_ACCOUNT_PRIVATE_KEY || '')
    wallet = wallet.connect(provider)
    const walletAddress = await wallet.getAddress()
    console.log(`Account airdrop: ${walletAddress}`)
    const airdropContract = new ethers.Contract(
        AIRDROP_ADDRESS,
        [
            'function dropTokens(address[] _recipients, uint256[] _amount) public returns (bool)',
            'function tokenAddress() public view  returns (address)'
        ],
        wallet
    );
    console.log(`Address of token accept: ${await airdropContract.tokenAddress()}`)

    const FROM_INDEX = 0
    const TO_INDEX = data.length -1
    for (let i = FROM_INDEX; i < TO_INDEX; i=i+BATCH_SIZE+1) {
        const toIdInBatch = (i+BATCH_SIZE > TO_INDEX) ? TO_INDEX : i+BATCH_SIZE;
        let arrAddress = []
        let arrAmount = []
        console.log(`fetch id from ${i} to ${toIdInBatch}`)
        for (let j = i; j <= toIdInBatch; j++) {
            arrAddress.push(data[j].address)
            arrAmount.push(ethers.utils.parseUnits(data[j].amount.toString()))
        }
        await airdropContract.dropTokens(arrAddress,arrAmount)

        totalAirdrop = totalAirdrop + arrAmount.reduce((a, b) => a + parseInt(ethers.utils.formatEther(b.toString())),0)
        totalUserAirdrop= totalUserAirdrop+ arrAddress.length

    }

    console.log(`totalAirdrop: ${totalAirdrop} , totalUser: ${totalUserAirdrop}`)

}
airdrop()