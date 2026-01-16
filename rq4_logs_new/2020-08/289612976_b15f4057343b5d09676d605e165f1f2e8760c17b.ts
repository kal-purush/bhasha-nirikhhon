import {
  find,
  findAll,
  guessCodeKey,
  fuzzySearch,
  fuzzySearchAll,
} from './countrycode'

test('find', () => {
  const defaultOrigin = find('Japan')
  const explicitOrigin = find('Japan', 'country.name.en')

  if (explicitOrigin === undefined || defaultOrigin === undefined) {
    throw 'test failed'
  }

  expect(defaultOrigin.iso2c).toEqual('JP')
  expect(explicitOrigin.iso2c).toEqual('JP')
})

test('findAll', () => {
  const defaultOrigin = findAll(['Japan'])
  const explicitOrigin = findAll(['Japan'], 'country.name.en')

  if (explicitOrigin.length === 0 || defaultOrigin.length === 0) {
    throw 'test failed'
  }

  if (explicitOrigin[0] === undefined || defaultOrigin[0] === undefined) {
    throw 'test failed'
  }

  expect(defaultOrigin[0].iso2c).toEqual('JP')
  expect(explicitOrigin[0].iso2c).toEqual('JP')
})

test('guessCodeKey default', () => {
  const results = guessCodeKey(['Japan', 'Antarctica'])

  const found = results.find((r) => r.key === 'country.name.en')
  if (found === undefined) {
    throw 'test failed'
  }
  expect(found.matchRatio).toEqual(1)
})

test('guessCodeKey iso3c', () => {
  const results = guessCodeKey(['JPN', 'ATA'], ['cowc', 'iso3c', 'gwc', 'p4c'])

  if (results.length === 0) {
    throw 'test failed'
  }

  expect(results[0].key).toEqual('iso3c')
  expect(results[0].matchRatio).toEqual(1)
})

test('fuzzySearch', () => {
  const results = fuzzySearch('jpn', 'country.name.en')

  if (results.length === 0 || results[0] === undefined) {
    throw 'test failed'
  }

  expect(results[0].iso2c).toEqual('JP')
})

test('fuzzySearch > empty string', () => {
  const results = fuzzySearch('', 'country.name.en')
  expect(results.length).toEqual(0)
})

test('fuzzySearchAll', () => {
  const results = fuzzySearchAll(['jpn', 'antartica'], 'country.name.en')

  expect(results[1][0]).toEqual('antartica')

  const antarticResults = results[1][1]
  if (antarticResults.length === 0 || antarticResults[0] === undefined) {
    throw 'test failed'
  }

  expect(antarticResults[0].iso2c).toEqual('AQ')
})