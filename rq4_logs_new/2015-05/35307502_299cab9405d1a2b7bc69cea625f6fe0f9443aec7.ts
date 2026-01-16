/// <reference path='../typings/tsd.d.ts' />

import lib = require('jpevarnek-demo-lib');
import stringifier = lib.stringifier;
import MessageGetter = lib.MessageGetter;

var obj = {
  a: 'foo',
  b: 'bar',
  c: 'baz',
};

var out = stringifier.stringify(obj);

var mg = new MessageGetter();

console.log(out);
console.log(mg.getMessage());