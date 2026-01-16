/**
 * SET YOUR VARIABLES
 */
let portfolio = {
    'AAPL': {
        shares: 2,
        price: 150
    },
    'GOOG': {
        shares: 1,
        price: 150
    }
};

let cash = 10;

let desiredAllocations = {
    'AAPL': 0.4,
    'GOOG': 0.4,
    'CASH': 0.2
};
/**
 * END
 */

function rebalancePortfolio(portfolio, cash, desiredAllocations) {
    portfolio['CASH'] = {
        shares: cash,
        price: 1 // Cash value always equals its amount
    };

    let totalValue = 0;
    for (let ticker in portfolio) {
        totalValue += portfolio[ticker].price * portfolio[ticker].shares;
    }

    console.log(`Total portfolio value: ${totalValue}$`);

    for (let ticker in portfolio) {
        let currentValue = portfolio[ticker].price * portfolio[ticker].shares;
        let currentPercentage = (currentValue / totalValue) * 100;
        console.log(`Current percentage of ${ticker}: ${currentPercentage.toFixed(2)}%`);
    }

    let transactions = {};

    for (let ticker in desiredAllocations) {
        let desiredValue = totalValue * desiredAllocations[ticker];
        let currentValue = (portfolio[ticker]?.price || 0) * (portfolio[ticker]?.shares || 0);
        let diffValue = desiredValue - currentValue;
        let diffShares = diffValue / (portfolio[ticker]?.price || 1); // Use 1 as default value for price to prevent division by zero

        if (diffShares > 0) {
            if (portfolio['CASH'].shares * portfolio['CASH'].price >= diffValue) {
                portfolio['CASH'].shares -= diffValue;
                transactions[ticker] = `Buy ${diffShares.toFixed(2)} shares of ${ticker}`;
            } else {
                transactions[ticker] = `Not enough cash to buy ${diffShares.toFixed(2)} shares of ${ticker}`;
            }
        } else if (diffShares < 0) {
            portfolio['CASH'].shares -= diffValue; // Add the amount to cash
            transactions[ticker] = `Sell ${(-diffShares).toFixed(2)} shares of ${ticker}`;
        } else {
            transactions[ticker] = `No action required for ${ticker}`;
        }
    }

    return transactions;
}


console.log(`Desired ratio : ${JSON.stringify(desiredAllocations)} \n`)

console.log(rebalancePortfolio(portfolio, cash, desiredAllocations));