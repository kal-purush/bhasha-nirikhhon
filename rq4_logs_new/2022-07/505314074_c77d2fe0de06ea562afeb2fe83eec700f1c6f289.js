const db = require('../Connection/Connection');
const util = require('util');
const query = util.promisify(db.query).bind(db);

var url = 'https://api.rajaongkir.com/starter';
var headers = '8a893732f389789a4c005911c2e314ba';
const axios = require('axios');

module.exports = {
  getProvince: (req, res) => {
    axios
      .get(`${url}/province?key=${headers}`)
      .then((response) => {
        res.send({
          error: false,
          massage: 'get data province success',
          data: response.data,
        });
      })
      .catch((err) => {
        console.log(error);
      });
  },
  getCity: (req, res) => {
    axios
      .get(`${url}/city?province=${req.headers.provinceid}&key=${headers}`)
      .then((response) => {
        res.send({
          error: false,
          massage: 'get data kota success',
          data: response.data,
        });
      })
      .catch((err) => {
        console.log(error);
      });
  },
};