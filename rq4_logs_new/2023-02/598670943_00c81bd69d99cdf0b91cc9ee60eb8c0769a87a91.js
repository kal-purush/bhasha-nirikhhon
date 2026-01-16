import * as dexapi from '../dexapi.js';
import { submitLimitOrder, ORDERSIDES } from '../dexrpc.js';
import { getConfig, getLogger } from '../utils.js';

// Trading config
const config = getConfig();
const { username } = config;
const riskConfig = config.get('riskstrategy');
const { riskManagementFactor, historicalDataCount, symbol, minimumTradingVolumePercentage, maximumVolatility } = riskConfig;
const bidToken = symbol.split("_")[1];
const askToken = symbol.split("_")[0];

const calculateAverage = (data, key) => {
    let sum = 0;
    data.forEach(element => {
        sum += element[key];
    });
    return sum / data.length;
};

const calculateStandardDeviation = (data, average, key) => {
    let sum = 0;
    data.forEach(element => {
        sum += Math.pow(element[key] - average, 2);
    });
    return Math.sqrt(sum / data.length);
};

const calculateVolatility = (average, standardDeviation) => {
    return standardDeviation / average;
};

const calculateTradingVolume = data => {
    let volume = 0;
    data.forEach(element => {
        volume += element.bid_amount + element.ask_amount;
    });
    return volume;
};

const calculateAverageTradingVolume = data => {
    return calculateTradingVolume(data) / data.length;
  };
  
  const getMinimumTradingVolume = data => {
    return minimumTradingVolumePercentage * calculateAverageTradingVolume(data);
  };
  
  const isTradingVolumeSufficient = data => {
    return calculateTradingVolume(data) >= getMinimumTradingVolume(data);
  };

const determineTrend = data => {
    const length = data.length;
    if (data[length - 1].price > data[length - 2].price) {
        return "BUY";
    } else if (data[length - 1].price < data[length - 2].price) {
        return "SELL";
    }
    return "SIDEWAYS";
};

const calculateOrderQuantity = (riskManagementFactor, capital, currentPrice, volatility) => {
    const quantity = (riskManagementFactor * capital) / (currentPrice * volatility * Math.sqrt(252));
    return Math.min(Math.floor(quantity), Math.floor(capital / currentPrice));
};

const getOptimalLevel = (side, orderBookData, tradingVolume) => {
    if (side === "BUY") {
        let totalBid = 0;
        for (const element of orderBookData.bids) {
            totalBid += element.bid * element.count;
            if (totalBid >= tradingVolume * 0.1) {
                return element.level;
            }
        }
    } else {
        let totalAsk = 0;
        for (const element of orderBookData.asks) {
            totalAsk += element.ask * element.count;
            if (totalAsk >= tradingVolume * 0.1) {
                return element.level;
            }
        }
    }
};

const executeTrade = async(side, count, level) => {
    await submitLimitOrder(symbol, side === "BUY" ? ORDERSIDES.BUY : ORDERSIDES.SELL, count, level);
    console.log(`Executing ${side} trade of ${count} shares at price ${level}`);
};

const getOpenOrders = async () => {
    const market = dexapi.getMarketBySymbol(symbol);
    if (market === undefined) {
      throw new Error(`Market ${symbol} does not exist`);
    }
    const allOrders = await dexapi.fetchOpenOrders(username);
    const orders = allOrders.filter((order) => order.market_id === market.market_id);
    await Promise.all(orders);
  
    return orders;
  };

const trade = async () => {
    const logger = getLogger();
    logger.info(`Executing ${symbol} risk strategy trades on account ${username}`);
  
    try {
        const openOrders = await getOpenOrders();

        if(openOrders.length > 0)
        {
            logger.info(`nothing to do - we have ${openOrders.length} orders on the books`);
            return;
        }
        const orderBookData = await dexapi.fetchOrderBook(symbol, 100);
        const historyData = await dexapi.fetchTrades(symbol, historicalDataCount);
        const tradingVolume = calculateTradingVolume(historyData);

        // check if trading volume is sufficient
        if (isTradingVolumeSufficient(historyData)) {
            logger.info(`Trading volume is sufficient, proceed with trade execution (${tradingVolume})`);
        } else {
            logger.info(`Trading volume is insufficient, abort trade execution (${tradingVolume})`);

            return;
        }

        const balances = await dexapi.fetchBalances(username);

        if(!balances)
        {
            logger.error(`You have no balances`);

            return;
        }

        const bidAmount = balances.find(balance => balance.currency === bidToken)?.amount;
        const askAmount = balances.find(balance => balance.currency === askToken)?.amount;
        const averagePrice = calculateAverage(historyData, "price");
        const standardDeviation = calculateStandardDeviation(historyData, averagePrice, "price");
        const volatility = calculateVolatility(averagePrice, standardDeviation);
        
        const trend = determineTrend(historyData);

        if(volatility > maximumVolatility)
        {
            logger.info(`Market volatility is too high to make a trade right now (${volatility})`);

            return;
        }

        if(trend === 'SIDEWAYS')
        {
            logger.info(`Trend is sideways.  Not making trade`);

            return;
        }

        logger.info(`You have ${bidAmount} ${bidToken} and ${askAmount} ${askToken}`);

        const amount = trend === "BUY" ? bidAmount : askAmount;
        const token = trend === "BUY" ? bidToken : askToken;

        if(!amount)
        {
            logger.error(`You have no ${token}`);

            return;
        }

        const capital = Number(amount);

        const currentPrice = historyData[historyData.length - 1].price;
        const orderQuantity = calculateOrderQuantity(riskManagementFactor, capital, currentPrice, volatility);

        console.log(`${capital} ${orderQuantity} ${riskManagementFactor} ${token}`)
        // Use the volatility and tradingVolume to adjust the order quantity
        //const adjustedOrderQuantity = Math.floor(orderQuantity * (1 + volatility) * (tradingVolume / capital));
        const optimalLevel = getOptimalLevel(trend, orderBookData, tradingVolume);

        await executeTrade(trend, orderQuantity, optimalLevel);
    } catch (error) {
      logger.error(error.message);
    }
  };
  
  const strategy = {
    trade,
  };
  
  export default strategy;