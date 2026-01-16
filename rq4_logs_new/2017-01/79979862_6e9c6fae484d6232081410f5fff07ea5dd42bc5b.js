/* Functions in this file should have no knowledge of the HTML. I'm hoping to
 * eventually port this as a script for Google Sheets as well, and separating
 * these functions should make that a lot easier.
 */

var index = function(str, num, zeroIndex) {
  // TODO: Use a global zeroIndex.
  if (!zeroIndex) { num--; }
  return str.charAt(num);
};

/**
 * Indexes all detected number columns into detected string columns.
 *
 * @param {!array} data
 * @param {bool} opt_zeroIndex If true, zero index. Default is false.
 * @returns {!array}
 */
var crossIndex = function(data, opt_zeroIndex) {
  var headers = getHeaders(data, true);
  var zeroIndex = opt_zeroIndex || false;
  var groups = groupCols(data);
  var numberCols = groups[0];
  var stringCols = groups[1];

  var output = [];

  var newHeaders = [];
  for (var numColI = 0; numColI < numberCols.length; numColI++) {
    var numCol = numberCols[numColI];
    for (var strColI = 0; strColI < stringCols.length; strColI++) {
      var strCol = stringCols[strColI];
      newHeaders.push(headers[strCol] + '.' + headers[numCol]);
    }
  }
  output.push(newHeaders);

  for (var i = 0; i < data.length; i++) {
    var newRow = [];
    for (var numColI = 0; numColI < numberCols.length; numColI++) {
      var numCol = numberCols[numColI];
      for (var strColI = 0; strColI < stringCols.length; strColI++) {
        var strCol = stringCols[strColI];
        newRow.push(index(data[i][strCol], data[i][numCol], zeroIndex));
      }
    }
    output.push(newRow);
  }
  return output;
};

var rotateLetter = function(word, rotateBy) {
  var output = '';
  for (var i = 0; i < word.length; i++) {
    var code = word.charCodeAt(i);
    var capitalLetter = 65 <= code && code <= 90;
    var lowercaseLetter = 97 <= code && code <= 122;
    if (capitalLetter || lowercaseLetter) {
      var offset = capitalLetter ? 65 : 97;
      var num = code - offset;
      num = ((num + rotateBy) % 26 + 26) % 26;
      output += String.fromCharCode(num + offset);
    } else {
      output += word[i];
    }
  }
  return output;
};

var rotateLetters = function(data, rotateBy) {
  var headers = getHeaders(data, true);
  var stringCols = groupCols(data)[1];
  var output = [];
  var newHeaders = [];
  for (var strColI = 0; strColI < stringCols.length; strColI++) {
    var strCol = stringCols[strColI];
    newHeaders.push(headers[strCol]);
  }
  output.push(newHeaders);

  for (var i = 0; i < data.length; i++) {
    var newRow = [];
    for (var strColI = 0; strColI < stringCols.length; strColI++) {
      var strCol = stringCols[strColI];
      newRow.push(rotateLetter(data[i][strCol], rotateBy));
    }
    output.push(newRow);
  }
  return output;
};