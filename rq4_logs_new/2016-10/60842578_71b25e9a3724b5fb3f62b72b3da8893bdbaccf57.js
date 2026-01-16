

var assert = require('assert');
var mox = require('../build/Debug/mox');

describe('Fitter', function() {
  it('approximate2d', function () {
    let answer = mox.ops.approximate2d([0,0],[20,30],[127,124],[210,150],[211,151],[25,170],[120,180],[200,200]);
    console.log(answer);
  })
});