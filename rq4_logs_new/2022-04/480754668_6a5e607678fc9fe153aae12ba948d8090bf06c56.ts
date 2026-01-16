// process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = '0';

export {}

const axios = require('axios');
require('dotenv').config();

const config = {
  method: 'get',
  url: `https://api.trello.com/1/boards/${process.env.BOARD_ID}/lists?key=${process.env.API_KEY}&token=${process.env.API_TOKEN}`,
};

axios(config)
.then(function (response: any) {
  response.data.forEach((list: any) => {
    console.log(`Name: ${list.name}\t| ID: ${list.id}`);
  })
})
.catch(function (error: any) {
  console.log(error);
});