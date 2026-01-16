import test from 'ava'

import { toHex } from './toHex'
import { getRGB } from './getRGB'

test('getRGBColor', t => {
  let a: string[] = []
  for (let i = 0; i <= 1; i += 0.05) {
    a.push(toHex(getRGB(i)))
  }

  t.deepEqual(a, ['#ff0000',
    '#ff4d00',
    '#ff9900',
    '#ffe600',
    '#ccff00',
    '#7fff00',
    '#33ff00',
    '#00ff19',
    '#00ff66',
    '#00ffb2',
    '#00ffff',
    '#00b3ff',
    '#0066ff',
    '#0019ff',
    '#3300ff',
    '#8000ff',
    '#cc00ff',
    '#ff00e5',
    '#ff0099',
    '#ff004c'])
})