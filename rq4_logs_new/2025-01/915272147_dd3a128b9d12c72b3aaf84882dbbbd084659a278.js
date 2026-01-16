import { Crypto } from "../models/crypto.models.js";
import { fetchFromApi } from "../utils/fetchFromApi.js";
import { getStandardDeviation } from "../utils/standarddeviation.js";

async function getCryptoStats(req, res) {
  const coin = req.params.coin;
  fetchFromApi([coin]).then((data) => {
    const {
      current_price: price,
      market_cap: marketCap,
      price_change_24h: twentyFourHChange,
    } = data[0];
    // res.json(data);
    res.json({ price, marketCap, "24hchange": twentyFourHChange });
  });
}

export { getCryptoStats };

async function cryptoDeviation(req, res) {
  const coin = req.params.coin;
  const data = await Crypto.find({ coin: coin }).exec();
  const price = data.map((entry) => entry.price);
  res.send({ deviation: getStandardDeviation(price) });
}

export { cryptoDeviation };