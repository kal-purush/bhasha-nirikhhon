const axios = require("axios")


async function getfirstThreeExchangeRate(){
    const currencyData = await getExchangeData();
    return currencyData;
}

async function getExchangeRateByCurrency(currency){
    
    const currencyData = await getExchangeData().filter((element) => element.moneda === currency);
    const finalCurrency = currencyData[0] ;
    delete finalCurrency.multiplicare;
    return finalCurrency;
    
}

async function getExchangeData() {
    try {
        const currentDate = new Date().getTime();
        console.log("Hello WORLD !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        const exchange = await axios.get(`https://www.zf.ro/js/valute.js?_=${currentDate}`,null,{ headers: { 'User-Agent': 'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1' }});
        console.log(exchange.headers);
        const formatedExchange = exchange?.data?.replace('valute = ', "");
        const sliceExchange = formatedExchange.slice(0, -2);
        const exchangeJSON = JSON.parse(sliceExchange);
        const currencyData = exchangeJSON[0].other_info.curs_valutar;
        return currencyData;
    } catch (error) {
        console.log(error);
        return error;
    }
   
}

module.exports={getfirstThreeExchangeRate,getExchangeRateByCurrency}