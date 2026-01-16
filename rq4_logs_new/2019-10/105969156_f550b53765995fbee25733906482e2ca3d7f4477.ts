import {idGenerator} from './idGenerator'

describe('idGenerator', () => {
  test('idGenerator()', () => {
    const getId1 = idGenerator()
    const getId2 = idGenerator()

    expect(getId1()).not.toBe(getId2())
    expect(getId1()).not.toBe(getId2())
    expect(getId1()).not.toBe(getId2())
  })
  test('idGenerator(false)', () => {
    const getId1 = idGenerator(false)
    const getId2 = idGenerator(false)

    expect(getId1()).not.toBe(getId2())
    expect(getId1()).not.toBe(getId2())
    expect(getId1()).not.toBe(getId2())
  })
  test('idGenerator(true)', () => {
    const getId1 = idGenerator(true)
    const getId2 = idGenerator(true)

    expect(getId1()).toBe(getId2())
    expect(getId1()).toBe(getId2())
    expect(getId1()).toBe(getId2())
  })
})