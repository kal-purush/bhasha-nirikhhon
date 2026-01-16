var request = require('request');
var prompt = require('prompt');
var xlsx = require('xlsx');

//var workbook = xlsx.readFile('GrowthStocks.xlsx');
var workbook = xlsx.readFile('test.xlsx');
var workbookJson = xlsx.utils.sheet_to_json(workbook.Sheets['Export']);
//console.log(workbookJson);
var finalStockInfo = {};
workbookJson.forEach(row => {
    var symbol = row.Symbol;
    console.log(symbol);
    finalStockInfo[symbol] = {};
    finalStockInfo[symbol]['ComprehensiveRating'] = row['Comp Rating'];
    getPriceAndVolume(symbol, finalStockInfo);
    getMovingAverage(symbol, 'EMA', finalStockInfo);
    getMovingAverage(symbol, 'SMA', finalStockInfo);
});
setTimeout(function(){console.log(finalStockInfo)}, 9000);


//Taking it in as a prompt
// prompt.start();
// prompt.get(['ticker'], (err, result) => {
//     var stocks = result.ticker.split(',');
//     var finalStockInfo = {};
//     stocks.forEach(stock => {
//         finalStockInfo[stock] = {};
//         getPriceAndVolume(stock, finalStockInfo);
//         getPriceAndVolume(stock, finalStockInfo);
//         getMovingAverage(stock, 'EMA', finalStockInfo);
//         getMovingAverage(stock, 'SMA', finalStockInfo);
//     });
//     setTimeout(function(){console.log(finalStockInfo)}, 2000);
// });

function getPriceAndVolume(ticker, finalStockInfo){
    var url = `https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=${ticker}&apikey=GYWUJPM4V0P0WGF8`;
    request(url, {json: true}, (err, res, body) => {
        if (err) { return console.log(err); }
        var dataKey = 'Time Series (Daily)';
        var data = res.body[dataKey],
        dates = Object.keys(data),
        mostRecentDateKey,
        mostRecentDate = 0;
        dates.forEach(date => {
            var tempDate = Date.parse(date);
            if(tempDate > mostRecentDate){
                mostRecentDate = tempDate;
                mostRecentDateKey = date;
            }
        });
        finalStockInfo[ticker].Price = res.body[dataKey][mostRecentDateKey]['4. close'];
        finalStockInfo[ticker].Volume = res.body[dataKey][mostRecentDateKey]['5. volume'];
        return {
            Price: res.body[dataKey][mostRecentDateKey]['4. close'],
            Volume: res.body[dataKey][mostRecentDateKey]['5. volume']
        };
    })
}







function getMovingAverage(ticker, type, finalStockInfo){
    var url = `https://www.alphavantage.co/query?function=${type}&symbol=${ticker}&interval=daily&time_period=55&series_type=close&apikey=GYWUJPM4V0P0WGF8`;
    request(url, {json: true}, (err, res, body) => {
        if (err) { return console.log(err); }
        var dataKey = `Technical Analysis: ${type}`
        var data = res.body[dataKey],
        dates = Object.keys(data),
        mostRecentDateKey,
        mostRecentDate = 0;
        dates.forEach(date => {
            var tempDate = Date.parse(date);
            if(tempDate > mostRecentDate){
                mostRecentDate = tempDate;
                mostRecentDateKey = date;
            }
        });
        finalStockInfo[ticker][type]  = res.body[dataKey][mostRecentDateKey][type];
        return res.body[dataKey][mostRecentDateKey][type];
    });
}













function getPricesAndVolume(listOfStocks){
    var url = `https://www.alphavantage.co/query?function=BATCH_STOCK_QUOTES&symbols=${listOfStocks}&apikey=GYWUJPM4V0P0WGF8`;
    request(url, {json: true}, (err, res, body) => {
        if (err) { return console.log(err); }
        console.log(res.body['Stock Quotes']);
        return res.body['Stock Quotes'];
    });
}