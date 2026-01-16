'use strict';
const AWS = require('aws-sdk');
const dynamo = new AWS.DynamoDB.DocumentClient();

module.exports.hello = async (event = { serialNumber: ' ', dateTime: '', activated: false, clientId: '', device: '', type: '', payload: {} }, context) => {

  const collection = "IoTCatalog"

  console.log('Received event:', JSON.stringify(event, null, 2));

  const params = {
    TableName: collection,
    Item: {
      "serialNumber": event.serialNumber,
      "timestamp": event.dateTime,
      "activated": event.activated,
      "clientId": event.clientId,
      "device": event.device,
      "type": event.type,
      "payload": event.payload
    }
  };

  console.log("Saving Telemetry Data");

  dynamo.put(params, function (err, data) {
    if (err) {
      console.error("Unable to add device. Error JSON:", JSON.stringify(err, null, 2));
      context.fail();
      return {
        statusCode: 5600,
        body: JSON.stringify(
          {
            "error": "Error saving the item",
            input: event,
          },
          null,
          2
        ),
      };
    } else {
      console.log("Data saved:", JSON.stringify(params, null, 2));

      return {
        statusCode: 200,
        body: JSON.stringify(
          {
            "message": "Item created in DB",
            input: event,
          },
          null,
          2
        ),
      };
    }
  });

};


