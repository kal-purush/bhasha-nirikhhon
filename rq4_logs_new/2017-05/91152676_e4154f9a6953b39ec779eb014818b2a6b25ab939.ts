import Bimage = require('../index')
import path = require('path')

describe('Bimage', () => {
  it('should exist', () => {
    expect(Bimage).toBeDefined()
    expect(Bimage).toBeTruthy()
    expect(typeof Bimage).toBe('function')
  })
})