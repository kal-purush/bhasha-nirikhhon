var express = require("express");
var app = express();
var axios = require("axios");
const WebSocket = require("ws");
var path = require("path");
const http = require("http");
const bodyParser = require('body-parser');

//Azure fetch data required module 
const CosmosClient = require("@azure/cosmos").CosmosClient;
const config = require("./config");
const TaskList = require("./routes/tasklist");
const TaskDao = require("./models/taskDao");
const logger = require("morgan");
const cookieParser = require("cookie-parser");

app.use(bodyParser.urlencoded({
  extended: false
}))


app.use(express.static(path.join(__dirname, "public")));
const server = http.createServer(app).listen(8080);

const wss = new WebSocket.Server({ server });

// Broadcast to all.
wss.broadcast = function broadcast(data) {
  //console.log("----my data ---", data);
  //console.log("----wss ---", wss);
  wss.clients.forEach(function each(client) {
    //console.log("client>>>>>>>>>>", client)
    client.send(data);
    if (client.readyState === WebSocket.OPEN) {
      try {
        //console.log("sending data " + data);
        client.send(data);
      } catch (e) {
        console.error(e);
      }
    }
  });
};

//Todo App:
const cosmosClient = new CosmosClient({
  endpoint: config.host,
  auth: {
    masterKey: config.authKey
  }
});
const taskDao = new TaskDao(cosmosClient, config.databaseId, config.containerId);
const taskList = new TaskList(taskDao);
taskDao
  .init(err => {
    console.error(err);
  })
  .catch(err => {
    console.error(err);
    console.error("Shutting down because there was an error setting up the database.");
    process.exit(1);
  });

app.post("/", async (req, res, next) => {

  // const customerData = await taskList.showProductsData(req, res, req.body.id);

  const customerData = await taskList.showRacks(req, res);
  //let cusData = customerData;
  wss.broadcast(JSON.stringify(customerData));
  res.end();
  //res.render('/', { customerData: customerData });
  //res.redirect('/', { 'ID': userID });
});

app.get("/", async (req, res, next) => {
  console.log("PASSED Data")
  const customerData = await taskList.showProductsAllData(req, res);

  //let cusData = customerData;
  wss.broadcast(JSON.stringify(customerData));

  res.end();
})

app.use(function (req, res /*, next*/) {
  res.redirect("/");
});
// console.log("Global Id has been init", globalId);
// const server = http.createServer(app);




// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.



// Connection string for the IoT Hub service
//
// NOTE:
// For simplicity, this sample sets the connection string in code.
// In a production environment, the recommended approach is to use
// an environment variable to make it available to your application
// or use an x509 certificate.
// https://docs.microsoft.com/azure/iot-hub/iot-hub-devguide-security
//
// Using the Azure CLI:
// az iot hub show-connection-string --hub-name {YourIoTHubName} --output table
// HostName={YourIoTHubName}.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey={YourSharedAccessKey}
// var connectionString = 'HostName=hcliothub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=m9GMCMmwhdvaK+L/cm4UUK05W2XSRxt4b9atOi8cCec=';
// var connectionString = 'HostName=azuredevtest-iot.azure-devices.net;DeviceId=MyCDevice;SharedAccessKeyName=iothubowner;SharedAccessKey=Z7sqao2etcmegxh5D5Nttk5aNgpghsuD5xhSlqT2NPU=';
//var connectionString =
//  "HostName=IoT-Hub-Innovation.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=GoStgJk+fFRlC7cIvV0QaoaEMbzU6UONRFvqV7UC0/o=";

// var connectionString =
// 	"HostName=PepsiCoIOT.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=H89Ly9g/fw4LuTc+9se0dNm77CbvXZW7ZRPrdHc81dQ="
var connectionString =
  "HostName=IoT-Hub-Innovation.azure-devices.net;DeviceId=pepsicodevice2;SharedAccessKey=5nYhtCOgk8ew/6u3lJvg255njOXsJNa/6bV5+BuGqso="

//HostName=IoT-Hub-Innovation.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=5nYhtCOgk8ew/6u3lJvg255njOXsJNa/6bV5+BuGqso=
//  Endpoint=sb://ihsuprodbyres043dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=GoStgJk+fFRlC7cIvV0QaoaEMbzU6UONRFvqV7UC0/o=;EntityPath=iothub-ehub-iot-hub-in-1184322-4dd8e7be6b
// Using the Node.js SDK for Azure Event hubs:
//   https://github.com/Azure/azure-event-hubs-node
// The sample connects to an IoT hub's Event Hubs-compatible endpoint
// to read messages sent from a device.
var { EventHubClient, EventPosition } = require("@azure/event-hubs");

var printError = function (err) {
  console.log(err.message);
};

// Display the message content - telemetry and properties.
// - Telemetry is sent in the message body
// - The device can add arbitrary application properties to the message
// - IoT Hub adds system properties, such as Device Id, to the message.
var printMessage = function (message) {
  console.log("Telemetry received: ");
  // console.log(JSON.stringify(message));
  console.log("-----", message.body);
  var data = message.body.toString("utf8");
  console.log("data recieved", data);
  // var obj = {
  //   body: (msg.data).toString('utf8'),
  //   properties: msg.properties.propertyList
  // }

  wss.broadcast(JSON.stringify(data));
  // console.log('Application properties (set by device): ')
  // console.log(JSON.stringify(message.applicationProperties));
  // console.log('System properties (set by IoT Hub): ')
  // console.log(JSON.stringify(message.annotations));
  console.log("");
};

// Connect to the partitions on the IoT Hub's Event Hubs-compatible endpoint.
// This example only reads messages sent after this application started.
var ehClient;
EventHubClient.createFromIotHubConnectionString(connectionString)
  .then(function (client) {
    console.log(
      "Successully created the EventHub Client from iothub connection string."
    );
    ehClient = client;
    return ehClient.getPartitionIds();
  })
  .then(function (ids) {
    console.log("The partition ids are: ", ids);
    return ids.map(function (id) {
      // return ehClient.send("hello world");

      return ehClient.receive(id, printMessage, printError, {
        eventPosition: EventPosition.fromEnqueuedTime(Date.now())
      });
    });
  })
  .catch(printError);