const axios = require("axios");

exports.handler = async function (event, context) {
  // empty response
  let responseData = {};

  // check if there was a payload sent in
  if (event?.body) {
    const requestBody = JSON.parse(event.body);
    // this is the path sent in
    const path = requestBody?.path;
    // request the data from there
    try {
      const response = await axios.get(path);
      responseData = response.data;
    } catch (e) {
      console.log(e);
    }
  }

  // forward the response
  return {
    statusCode: 200,
    body: JSON.stringify(responseData),
  };
};