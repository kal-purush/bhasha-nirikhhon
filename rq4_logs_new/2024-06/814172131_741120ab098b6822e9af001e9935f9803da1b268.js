const { Web3 } = require('web3');
const { abi, testnetAddresses } = require('./constantAbi');

async function getReserveData(rpcUrl, addressContract, addressUsdc) {
    const web3 = new Web3(rpcUrl);

    const contract = new web3.eth.Contract(abi, addressContract);
    const data = await contract.methods.getReserveData(addressUsdc).call();

    const decimalRate = Number(data.currentLiquidityRate) / Math.pow(10, 27);

    const continuousRate = Math.log(1 + decimalRate);

    const apy = 100 * (Math.exp(continuousRate) - 1);
    return apy;
}

async function main() {
    let final_result, chainName, selector
    for (const [name, value] of Object.entries(testnetAddresses)) {
        const result = await getReserveData(value['rpcUrl'], value['addressContract'], value['addressUsdc']);
        console.log(`For chain ${name} APY: ${result.toFixed(2)}%`)
        if (final_result > result) {
            continue;
        }
        final_result = result;
        chainName = name;
        selector = value['selector'];
    }
    console.log(`\nBest APY in ${chainName} ${final_result.toFixed(2)}%\nChain selector: ${selector}`)


}
main()