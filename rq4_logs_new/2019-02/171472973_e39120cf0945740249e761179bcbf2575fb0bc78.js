const axios = require('axios');


//Helpers
const calculateExchange = (amount, rate) => {
    return Math.round((amount * rate) * 100) / 100;
}

//Handlers

exports.getHomePage = (req, res, next) => {
    res.render('home');
}

exports.getExchangeRate = (req, res, next) => {

    const query = req.query;
    const amount = query.a,
        from = query.from.toUpperCase(),
        to = query.to.toUpperCase();

    axios.get(`https://api.exchangeratesapi.io/latest?base=${from}`)
        .then((response) => {
            const rate = response.data.rates[to];
            const result = calculateExchange(amount, rate);
            res.render('results', {
                amount,
                result,
                currency: {
                    from,
                    to,
                    rate: calculateExchange(1, rate)
                }
            })

        })
        .catch((e) => {
            console.log(e);
            res.render('404');
        });
}