import test from 'japa'

import parseMoney from '../../utils/parseMoney'

test('should parseMoney return a string formatted with currency BRL format', (assert) => {
  const formatedNumber = parseMoney(1000)

  assert.isString(formatedNumber)
  assert.include(formatedNumber, '1.000,00')
})