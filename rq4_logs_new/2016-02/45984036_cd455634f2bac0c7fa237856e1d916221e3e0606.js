/**
 * This file is part of the "zui" project
 *   Copyright (c) 2016, zScale Technologies GmbH - All rights reserved.
 *
 * Authors:
 *   Laura Schlimmer <laura@zscale.io>
 *   Paul Asmuth <paul@zscale.io>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the zScale Technologies GmbH nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL ZSCALE TECHNOLOGIES GMBH BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

zCSVUtil = this.zCSVUtil || {};

zCSVUtil.toCSV = function(opts, columns, rows) {
  opts = opts || {};
  opts.row_separator = opts.row_separator || "\n";
  opts.column_separator = opts.column_separator || ",";
  opts.quote_char = opts.quote_char || "\"";

  var csv = "";
  var add_row = function(row) {
    var r = row.map(function(s) {
      s = (s || "").toString();

      while (s.indexOf(opts.quote_char) >= 0) {
        s = s.replace(opts.quote_char, "\\" + opts.quote_char);
      }

      return opts.quote_char + s + opts.quote_char;
    })

    csv += r.join(opts.column_separator) + opts.row_separator;
  }

  add_row(columns);

  rows.forEach(function(row) {
    var r = [];
    for (var i = 0; i < columns.length; ++i) {
      var cell = row[i];
      r.push(cell ? cell : null);
    }
    add_row(r);
    });

    return csv;
  }
