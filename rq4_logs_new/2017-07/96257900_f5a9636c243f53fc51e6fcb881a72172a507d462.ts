import test from 'ava';

import gmaps = require('@google/maps');

test('with promise', t => {
  gmaps.createClient({
    key: '',
    Promise
  });
  t.pass();
});