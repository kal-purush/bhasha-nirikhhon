'use strict';

const express = require('express');
var prometheus = require('prom-client');
const prefix = 'orders_svc_';
prometheus.collectDefaultMetrics({ prefix });

const app = express();

app.listen(8080, function () {
    console.log('order-svc started on port 8080');
})

const orders_placed = new prometheus.Counter({
    name: 'order_svc:orders_placed',
    help: 'Total orders'
});

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

app.get('/order', function (req, res) {
    let orderNumber = (Math.floor(Math.random() * 1000) + 1).toString();
    res.send('Thank you for your order! Your order id is ' + orderNumber + '\n');
    orders_placed.inc();
})

app.get('/rating', function (req, res) {
    let rating = (Math.floor(Math.random() * 4) + 1).toString();
    res.send('You rated the order process ' + rating + ' stars. Thank you for your feedback!\n');
})

app.get('/metrics', function (req, res) {

    res.set('Content-Type', prometheus.register.contentType);
    res.send(prometheus.register.metrics());
})