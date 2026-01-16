'use strict';

const render = require('./render');

function getRegisterPage(options) {
  return function* () {

    let users = yield User.find({}).lean();

    yield render.index(users, 'list.dust');

  }
}