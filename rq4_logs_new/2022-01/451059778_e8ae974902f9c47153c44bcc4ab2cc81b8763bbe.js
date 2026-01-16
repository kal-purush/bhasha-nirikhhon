const rateLimit = require('express-rate-limit');
const { err, wrapper } = require('../util/response');

module.exports = (minutes, max, message) => {
  return rateLimit({
    windowMs: minutes * 60 * 1000,
    max, // Limit each IP to max requests per `window` (here, per windowMs)
    standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
    legacyHeaders: false, // Disable the `X-RateLimit-*` headers
    handler: (_, res) => {
      return wrapper(res, err({ message }));
    }
  });
};