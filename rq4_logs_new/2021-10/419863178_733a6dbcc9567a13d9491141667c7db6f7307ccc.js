const axios = require('axios');

axios({
  method: 'get',
  url: `https://artginzburg.runkit.io/turnakolsky-webhooks/branches/master/${process.env.BITRIX_INCOMING_WEBHOOK}/check`,
});