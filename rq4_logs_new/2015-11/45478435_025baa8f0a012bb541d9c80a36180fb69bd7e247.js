'use strict';

let request = require('request');

let sumVariants = function(partialPrice, variant) {
  return partialPrice + Number(variant.price);
};

let sumProducts = function(partialPrice, product) {
  return partialPrice + product.variants.reduce(sumVariants, 0);
};

let sumLampsAndWallets = function(products) {
  return products.filter((p) => {
    return p.product_type === 'Wallet' ||
           p.product_type === 'Lamp';
  }).reduce(sumProducts, 0);
};

request('http://shopicruit.myshopify.com/products.json',
        (error, response, body) => {
          if (error) return console.log(error);
          let products = JSON.parse(body).products;
          let costLampAndWallets = sumLampsAndWallets(products);
          console.log(`$ ${costLampAndWallets}`);
        });