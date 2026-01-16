/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

/**
 * Adapted for TypeScript by Zalando / Team Norris
 *
 * See https://github.com/facebookincubator/create-react-app/tree/master/packages/react-dev-utils
 * for original implementation.
 */

// This alternative WebpackDevServer combines the functionality of:
// https://github.com/webpack/webpack-dev-server/blob/webpack-1/client/index.js
// https://github.com/webpack/webpack/blob/webpack-1/hot/dev-server.js

// It only supports their simplest configuration (hot updates on same server).
// It makes some opinionated choices on top, like adding a syntax error overlay
// that looks similar to our console output. The error overlay is inspired by:
// https://github.com/glenjamin/webpack-hot-middleware

import formatWebpackMessages from './formatWebpackMessages';

const ansiHTML = require('ansi-html');
const SockJS = require('sockjs-client');
const stripAnsi = require('strip-ansi');
const url = require('url');
const Entities = require('html-entities').AllHtmlEntities;
const entities = new Entities();

console.log(ansiHTML);
console.log(SockJS);
console.log(stripAnsi);
console.log(url);
console.log(Entities);
console.log(entities);

export default () => {

};