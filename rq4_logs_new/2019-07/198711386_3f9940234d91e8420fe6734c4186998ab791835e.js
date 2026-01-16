'use strict';
const alfy = require('alfy');

const input = alfy.input.split(' '),
  number = input[0],
  currency = (input[1]) 
                ? String(input[1]).toUpperCase() 
                : 'EUR,USD',
  api = 'https://api.ratesapi.io/api/latest?base=RUB';

let query = '&symbols=' + currency;

const url = api + query,
  rub = await alfy.fetch(url);

let result = [];

for (let key in rub.rates) {
  result.push({
    title: number / rub.rates[key],
    subtitle: key,
  });
}

alfy.output(result);