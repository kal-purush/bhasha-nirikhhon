var util = require('util'),
    logger = require('winston'),
    CoinMarketCap = require("node-coinmarketcap");;



module.exports = function(client, router)
{
  var coinmarketcap = new CoinMarketCap({
    events : true,
    refresh: 60,
    convert: "EUR"
    });

  coinmarketcap.on("ripple", coin => {
    client.publish("crypto/coin", coin.price_eur + "", { retain : true });
  });



}