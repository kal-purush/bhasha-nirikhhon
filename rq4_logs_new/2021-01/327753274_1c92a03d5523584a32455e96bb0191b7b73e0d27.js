const express = require('express');
const router = express.Router();
const Openpay = require('openpay');
/* GET users listing. */
router.get('/', (req, res, next) => {
  const openpay = new Openpay(process.env.OPEN_PAY_ID, process.env.OPEN_PAY_SK, false);
  openpay.charges.list((error, list) => {
    if (error) {
      console.error(error);
      res.status(403).send('Openpay error');
    } else {
      console.log(list);
      res.send(list);
    }
  });
});

module.exports = router;