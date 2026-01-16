/**
 * Copyright (C) 2019 Yudha Tama Aditiyara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
'use strict';

const Base64 = require("ganiyem-util-base64");

/**
 * @param {string} data
 * @returns {string}
 */
function pad(data){
	return data + "=".repeat(4 - ((data.length % 4) || 4));
}

/**
 * @param {string} data
 * @returns {string}
 */
function escape(data){
	return data.replace(/\+/g,'-').replace(/\//g,'_').replace(/\=/g, '');
}

/**
 * @param {string} data
 * @returns {string}
 */
function unescape(data){
	return pad(data).replace(/\-/g,'+').replace(/\_/g,'/');
}

/**
 * @param {string|number} data
 * @param {string|void} encoding
 * @returns {string}
 */
function encode(data, encoding){
	return escape(Base64.encode(data, encoding));
}

/**
 * @param {string} data
 * @param {string|void} encoding
 * @returns {string}
 */
function decode(data, encoding){
	return Base64.decode(unescape(data), encoding);
}

/**
 */
module.exports = {pad, escape, unescape, encode, decode};