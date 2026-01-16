import test from 'ava'

import * as colormap from 'colormap'

import { createRainbow } from './createRainbow'

test.only('match colormap rainbow', t => {
  const expected = colormap({
    colormap: 'rainbow',
    nshades: 72,
    format: 'rgb'
  })
  // console.log(expected)
  const actual = createRainbow(72, [1, 1])
  console.log(actual.length)
  t.deepEqual(actual, expected)
})