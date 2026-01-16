'use strict';
module.exports = clefpadder;

function clefpadder(str, clefs) {
    Array(clefs).fill(true).forEach(() => {
      str = "🎼"+str+"🎼";
    });
    return str;
}