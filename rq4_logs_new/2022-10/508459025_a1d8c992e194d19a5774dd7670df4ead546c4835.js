let express = require("express");
let router = express.Router();
const axios = require("axios");
const client = require("../bin/redis-client");
const amqp = require("amqplib");

const rabbitSettings = {
  protocol: "amqp",
  hostname: "20.242.102.13",
  port: 5672,
  username: "gabricauser",
  password: "SrfConsultores2020***",
  authMechanism: ["PLAIN", "AMQPLAIN", "EXTERNAL"],
  vhost: "/",
};

connect();

async function connect() {
  try {
    const conn = await amqp.connect(rabbitSettings);
    const channel = await conn.createChannel();

    channel.consume("SalesOrders", async (msg) => {
      const body = JSON.parse(msg.content.toString());

      const tenantUrl = body && body.tenantUrl;
      const clientId = body && body.clientId;
      const clientSecret = body && body.clientSecret;
      const tenant = body && body.tenant;
      const environment = body && body.environment;
      const salesOrder = body && body.salesOrder;
      const salesOrderLine = body && body.salesOrderLine;

      if (!client.isOpen) client.connect();

      let token = await client.get(environment);

      if (!token) {
        const tokenResponse = await axios
          .post(
            `https://login.microsoftonline.com/${tenantUrl}/oauth2/token`,
            `grant_type=client_credentials&client_id=${clientId}&client_secret=${clientSecret}&resource=${tenant}/`,
            { headers: { "Content-Type": "application/x-www-form-urlencoded" } }
          )
          .catch(function (error) {
            if (
              error.response &&
              error.response.data &&
              error.response.data.error &&
              error.response.data.error.innererror &&
              error.response.data.error.innererror.message
            ) {
              throw new Error(error.response.data.error.innererror.message);
            } else if (error.request) {
              throw new Error(error.request);
            } else {
              throw new Error("Error", error.message);
            }
          });
        token = tokenResponse.data.access_token;
        await client.set(environment, tokenResponse.data.access_token, {
          EX: 3599,
        });
      }

      let _salesOrderLine = [];

      if (salesOrderLine && salesOrderLine.length > 0) {
        let SalesOrderLinesGet = [];

        for (let i = 0; i < salesOrderLine.length; i++) {
          const line = salesOrderLine[i];

          //Entidad Extendida
          const SalesOrderLinesItem = axios.post(
            `${tenant}/data/CDSSalesOrderLinesV2?cross-company=true`,
            {
              SalesOrderNumber: salesOrder.SalesOrderNumber,
              SalesOrderNumberHeader: salesOrder.SalesOrderNumber,
              dataAreaId: salesOrder.dataAreaId,
              ...line,
            },
            { headers: { Authorization: "Bearer " + token } }
          );

          SalesOrderLinesGet.push(SalesOrderLinesItem);
        }

        await axios
          .all(SalesOrderLinesGet)
          .then(
            axios.spread(async (...responses2) => {
              for (let i = 0; i < responses2.length; i++) {
                const element = responses2[i];
                _salesOrderLine.push(element.data);
              }
            })
          )
          .catch(function (error) {
            if (
              error.response &&
              error.response.data &&
              error.response.data.error &&
              error.response.data.error.innererror &&
              error.response.data.error.innererror.message
            ) {
              console.log(error.response.data.error.innererror);
              throw new Error(error.response.data.error.innererror.message);
            } else if (error.request) {
              throw new Error(error.request);
            } else {
              throw new Error("Error", error.message);
            }
          });
      }

      let _salesOrder;

      if (salesOrder) {
        //Entidad Extendida
        _salesOrder = await axios
          .post(
            `${tenant}/api/services/GAB_SalesOrderConfirmationSG/GAB_SalesOrderConfirmationService/confirmSO`,
            {
              _salesId: salesOrder.SalesOrderNumber,
              _dataAreaId: salesOrder.dataAreaId,
            },
            {
              headers: { Authorization: "Bearer " + token },
            }
          )
          .catch(function (error) {
            if (
              error.response &&
              error.response.data &&
              error.response.data.error &&
              error.response.data.error.innererror &&
              error.response.data.error.innererror.message
            ) {
              throw new Error(error.response.data.error.innererror.message);
            } else if (error.request) {
              throw new Error(error.request);
            } else {
              throw new Error("Error", error.message);
            }
          });
      }

      _salesOrder = _salesOrder.data;

      channel.ack(msg);
    });
  } catch (error) {
    console.error(error);
  }
}

module.exports = connect;