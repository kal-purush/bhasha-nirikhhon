const dotEnv = require('dotenv');
let parsedEnv = dotEnv.config({ path: '.env' }).parsed;
if (process.env.NODE_ENV === 'production') {
  parsedEnv = dotEnv.config({ path: '.env.production' }).parsed;
}
module.exports = () => parsedEnv;