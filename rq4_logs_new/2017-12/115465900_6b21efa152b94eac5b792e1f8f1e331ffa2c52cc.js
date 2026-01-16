'use strict'

const accounts = require('./accounts.json');

const bittrex = require('node-bittrex-api');
bittrex.options({
  apikey: accounts.bittrex.apiKey,
  apisecret: accounts.bittrex.apiSecret,
  inverse_callback_arguments: true
});
bittrex.getbalancesAsync = function () {
  return new Promise((resolve, reject) => {
    bittrex.getbalances((err, accounts) => {
      if (err) {
        reject(err);
      } else {
        resolve(accounts);
      }
    });
  });
};
bittrex.getmarketsummariesAsync = function (symbol) {
  return new Promise((resolve, reject) => {
    bittrex.getmarketsummaries((err, data) => {
      if (err) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

const AuthenticatedClient = require('gdax').AuthenticatedClient;
const gdax = new AuthenticatedClient(accounts.gdax.apiKey, accounts.gdax.apiSecret, accounts.gdax.passphrase);

const BinanceRest = require('binance').BinanceRest;
const binance = new BinanceRest({
  key: accounts.binance.apiKey,
  secret: accounts.binance.apiSecret
});

function getAllAccounts() {
  return Promise.all([
    gdax.getCoinbaseAccounts(),
    gdax.getAccounts(),
    bittrex.getbalancesAsync(),
    binance.account()
  ])
  .then(accounts => {
    var [coinbase, gdax, bittrex, binance] = accounts;
    var result = {
      coinbase: formatCoinbase(coinbase),
      gdax: formatCoinbase(gdax),
      bittrex: formatBittrex(bittrex),
      binance: formatBinance(binance)
    };
    var total = {};
    for (const exchange in result) {
      result[exchange].reduce((acc, cur) => {
        if (acc[cur.currency]) {
          acc[cur.currency].balance += cur.balance;
        } else {
          acc[cur.currency] = {};
          acc[cur.currency].balance = cur.balance;
        }
        return acc;
      }, total);
    }
    result.total = total;
    return result;
  })
  .then(accounts => {
    return Promise.all([
      gdax.getProductTicker('BTC-USD'),
      gdax.getProductTicker('BCH-USD'),
      gdax.getProductTicker('ETH-USD'),
      gdax.getProductTicker('LTC-USD'),
      bittrex.getmarketsummariesAsync(),
      binance.allPrices(),
      accounts.total
    ]);
  })
  .then(tickers => {
    var [btc, bch, eth, ltc, bittrexAll, binanceAll, accounts] = tickers;
    var prices = {};
    prices.BTCUSD = { price: parseFloat(btc.price), source: 'gdax' };
    prices.BCHUSD = { price: parseFloat(bch.price), source: 'gdax' };
    prices.ETHUSD = { price: parseFloat(eth.price), source: 'gdax' };
    prices.LTCUSD = { price: parseFloat(ltc.price), source: 'gdax' };
    binanceAll.reduce((acc, cur) => {
      if (cur.symbol.match(/BTC$/)) {
        acc[cur.symbol] = { price: parseFloat(cur.price), source: 'binance' };
      }
      return acc;
    }, prices);
    bittrexAll.result.reduce((acc, cur) => {
      if (cur.MarketName.match(/^BTC/)) {
        var name = cur.MarketName.split('-')[1] + 'BTC';
        if (!acc[name] || (acc[name] && acc[name].price < cur.Last)) {
          acc[name] = { price: cur.Last, source: 'bittrex' };
        }
      }
      return acc;
    }, prices)

    var total = { USD: 0, BTC: 0 };
    for (const currency in accounts) {
      switch (currency) {
        case 'USD':
          total.USD += accounts['USD'].balance;
          break;
        case 'BTC':
          total.BTC += accounts['BTC'].balance;
          accounts[currency].USD = prices[currency + 'USD']
          accounts[currency]['usd-value'] = accounts[currency].balance * prices[currency + 'USD'].price
          break;
        case 'BCH':
        case 'ETH':
        case 'LTC':
          accounts[currency].USD = prices[currency + 'USD']
          accounts[currency]['usd-value'] = accounts[currency].balance * prices[currency + 'USD'].price
          total.USD += accounts[currency]['usd-value'];
          break;
        default:
          accounts[currency].BTC = prices[currency + 'BTC']
          accounts[currency]['btc-value'] = accounts[currency].balance * prices[currency + 'BTC'].price
          accounts[currency]['usd-value'] = accounts[currency]['btc-value'] * prices['BTCUSD'].price
          total.BTC += accounts[currency]['btc-value'];
      }
    }

    total.USD += (total.BTC * prices['BTCUSD'].price);
    return {
      total: total,
      accounts: accounts
    };
  });
};

function formatCoinbase(accounts) {
  var result = [];
  accounts.forEach(account => {
    var balance = parseFloat(account.balance);
    if (balance > 0) {
      result.push({
        currency: account.currency,
        balance: balance
      })
    }
  });
  return result;
};

function formatBittrex(accounts) {
  var result = [];
  accounts.result.forEach(account => {
    var balance = parseFloat(account.Balance);
    if (balance > 0) {
      result.push({
        currency: account.Currency,
        balance: balance
      })
    }
  });
  return result;
};

function formatBinance(accounts) {
  var result = [];
  accounts.balances.forEach(account => {
    var free = parseFloat(account.free);
    var locked = parseFloat(account.locked);
    if (free > 0 || locked > 0) {
      result.push({
        currency: account.asset,
        balance: free + locked
      })
    }
  });
  return result;
};

module.exports = {
  getAllAccounts: getAllAccounts
};