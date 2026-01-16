/*
    Partout [Everywhere] - Policy-Based Configuration Management for the
    Data-Driven-Infrastructure.

    Copyright (C) 2016 Graham Lee Bevan <graham.bevan@ntlworld.com>

    This file is part of Partout.

    Partout is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*jslint node: true, nomen: true, vars: true*/
/*jshint multistr: true*/
'use strict';

var console = require('better-console'),
    u = require('util');

/**
 * Assertions utils
 *
 * @mixin
 */
var UtilsEscapes = function () {

};

UtilsEscapes.prototype.escapeBackSlash = function (s) {
  return s.replace(/\\/g, '\\\\');
};

UtilsEscapes.prototype.winEscapeSpaces = function (s) {
  return s.replace(/ /g, '^ ');
};

module.exports = UtilsEscapes;