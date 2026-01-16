const WEB3_URL = `http://127.0.0.1:9545/`;

// Global variable
let web3, accounts, balances, borrowIndex ,payerIndex, borrower, payer;
let owner, contractAddress, debts;
let simpleLoan;
const DEFAULT_OPTION = -1;

async function init() {
    const provider = new Web3.providers.HttpProvider(WEB3_URL);
    web3 = new Web3(provider);
    await populateAccountTable();
    await deployCintract();
}

async function deployCintract() {
    $.getJSON('SimpleLoan.json', async contractABI => {
        const contract = TruffleContract(contractABI);
        contract.setProvider(web3.currentProvider);
        try {
            simpleLoan = await contract.deployed();
            contractAddress = simpleLoan.address;
            console.log('simmple loan contract : ', simpleLoan)
        }catch (err) {
            console.log(err);
        }
    });
}

async function populateAccountTable() {
    // ดึงข้อมูล account
    try 
    {
        accounts = await web3.eth.getAccounts();
        await getBalance();
        if (Array.isArray(accounts) && accounts.length > 0) {
            let htmlStr = '';
            for (let index = 0; index < accounts.length; index++) {
                const balanceEth = await web3.utils.fromWei(balances[index], 'ether');
                htmlStr += '<tr>';
                htmlStr += `<th scope="row">${index + 1}</th>`;
                htmlStr += `<td>${accounts[index]}</td>`;
                htmlStr += `<td>${Number(balanceEth).toFixed(8)}</td>`;
                htmlStr += `<td> + '' + </td>`;
                htmlStr += '</tr>';
            }

            $('#accountList').html(htmlStr);
        }
    }
    catch (err) {
        console.log(err);
    }
    
}

async function getBalance() {
    balances = await Promise.all(accounts.map(async account => await web3.eth.getBalance(account)));
}