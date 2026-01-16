import test from 'node:test'
import assert from 'node:assert'
import { transform, generateKey } from '../src/index.js'

test.describe('test transform code', () => {
  test.it('test variable declaration using var', async () => {
    const code = "var a = '字面量';"
    const actual = await transform(code)
    const expected = `var a = i18n("${generateKey('字面量')}");`
    assert.equal(actual, expected)
  })

  test.it('test variable declaration using let', async () => {
    const code = "let a = '字面量';"
    const actual = await transform(code)
    const expected = `let a = i18n("${generateKey('字面量')}");`
    assert.equal(actual, expected)
  })

  test.it('test variable declaration using const', async () => {
    const code = "const a = '字面量';"
    const actual = await transform(code)
    const expected = `const a = i18n("${generateKey('字面量')}");`
    assert.equal(actual, expected)
  })
})