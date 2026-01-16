'use strict';

const Promise = require("bluebird");
const crypto = require('crypto');
const logger = require('winston');

exports.generate = function () {
  const resetTokenLengthInBytes = 20;
  const randomBytes = Promise.promisify(crypto.randomBytes);
  return randomBytes(resetTokenLengthInBytes).then((bytes) => {
    return bytes.toString('hex');
  }).catch((err) => {
      logger.warn('Błąd podczas generowania tokenu resetowania hasła: %s', err);
      throw err;
  });
}