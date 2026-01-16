const https = require('https');
const schedule = require('node-schedule');
const { getDefaultProvider, Wallet, utils, BigNumber } = require('ethers');
const { parseBytes32String, formatUnits } = utils;
const Pool = require('pg-pool');
const { getNetwork } = require("./networks");
const { getContract } = require('./contractFactory');
const { parseEther, formatEther, parseUnits } = require('ethers/lib/utils');
const { WALLET_PRIVATE_KEY } = process.env;

const EUROS_ADDRESS = '0x643b34980E635719C15a2D4ce69571a258F940E9';
const TST_ADDRESS = '0xf5A27E55C748bCDdBfeA5477CB9Ae924f0f7fd2e';

const {
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USERNAME, POSTGRES_PASSWORD
} = process.env;

let pool = new Pool({
  database: POSTGRES_DB,
  user: POSTGRES_USERNAME,
  password: POSTGRES_PASSWORD,
  host: POSTGRES_HOST,
  port: POSTGRES_PORT
});

const convert = (amount, price, dec) => {
  return Number(formatUnits(amount.mul(price)
    .div(BigNumber.from(10).pow(8)), dec)).toFixed(2);
};

const fetchDexPrice = async address => {
  return new Promise(resolve => {
    https.get(`https://api.dexscreener.com/latest/dex/tokens/${address}`, res => {
      let json = '';

      res.on('data', data => {
        json += data;
      });

      res.on('end', _ => {
        resolve(parseUnits(JSON.parse(json).pairs[0].priceUsd, 8).toString());
      });
    });
  });
};

const fetchPrices = async (networkName, wallet) => {
  const prices = {};
  prices.EUROs = await fetchDexPrice(EUROS_ADDRESS);
  prices.TST = await fetchDexPrice(TST_ADDRESS);
  const tokens = await (await getContract(networkName, 'TokenManager')).connect(wallet).getAcceptedTokens();
  for (let i = 0; i < tokens.length; i++) {
    prices[tokens[i].symbol] = (await (await getContract(networkName, 'Chainlink', tokens[i].clAddr)).connect(wallet).latestRoundData()).answer.toString();
  }
  return prices;
};

const scheduleLiquidationPoolData = async _ => {
  schedule.scheduleJob('10 * * * *', async _ => {
    console.log('indexing liquidation pool snapshots')
    const network = getNetwork('arbitrum');
    const provider = new getDefaultProvider(network.rpc);
    const wallet = new Wallet(WALLET_PRIVATE_KEY, provider);
    const liquidationPool = await (await getContract(network.name, 'LiquidationPool')).connect(wallet);
    const prices = await fetchPrices(network.name, wallet);
    let i = 0;
    while (true) {
      try {
        const holderAddress = await liquidationPool.holders(i);
        const { _position, _rewards } = await liquidationPool.position(holderAddress);
        const rewardsArray = _rewards.map((reward) =>
          [parseBytes32String(reward.symbol), formatUnits(reward.amount, reward.dec), convert(reward.amount, prices[reward.symbol], reward.dec)]);
        const assets = [
          ['TST', formatEther(_position.TST), convert(_position.TST, prices.TST, 18)],
          ['EUROs', formatEther(_position.EUROs), convert(_position.EUROs, prices.EUROs, 18)],
          ...rewardsArray
        ];

        const now = new Date();
        const client = await pool.connect();
        try {
          const query = 'INSERT INTO user_pool_snapshots (user_address,assets,snapshot_at) VALUES ($1,$2,$3);';
          const data = [
            holderAddress.toLowerCase(),
            assets,
            now
          ];
          await client.query(query, data);
        } catch (e) {
          console.log(e);
        } finally {
          client.release();
          i++;
        }
      } catch {
        break;
      }
    }
  });
};

module.exports = {
  scheduleLiquidationPoolData
};