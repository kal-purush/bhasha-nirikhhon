const pizzas = require('../data');
const AWS = require('aws-sdk');
const docClient = new AWS.DynamoDB.DocumentClient();
const rp = require('minimal-request-promise');

module.export.deletePizzas = (id) => {
  if (!id) {
    throw new Error('Order ID is required for deleting the order');
  }
  return docClient.get({
    TableName: 'pizza-orders',
    Key: {
      orderId: id
    }
  }).promise()
  .then(result => result.Item)
  .then(item => {
    if (item.orderStatus !== 'pending') {
      throw new Error('Order status is not pending');
    }
    return rp.delete('https://some-like-it-hot-api.effortless-serverless.com/delivery', {
      headers: {
        "Authorization": "aunt-marias-pizzeria-1234567890",
        "Content-type": "application/json"
      }
    });
  })
  .then(() => {
    return docClient.delete({
      TableName: 'pizza-orders',
      Key: {
        orderId: orderId
      }
    }).promise()
  });
};