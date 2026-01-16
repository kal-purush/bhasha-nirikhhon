var express = require('express');
var multiparty = require('multiparty');
const fse = require('fs-extra');
const path = require('path');
const fs = require('fs');

var router = express.Router();

const DOWNLOAD_FILE = path.resolve(__dirname, '../public/upload/test.docx')

router.post('/', async function (req, res, next) {
  const readStream = fs.createReadStream(DOWNLOAD_FILE);
  const buffers = [];
  readStream.on('data', function(buffer) {
    buffers.push(buffer);
  });
  readStream.on('end', function() {
    console.log(buffers);
    // const data = Buffer.from(buffers);
    res.send(Buffer.concat(buffers));
  });
});

module.exports = router;