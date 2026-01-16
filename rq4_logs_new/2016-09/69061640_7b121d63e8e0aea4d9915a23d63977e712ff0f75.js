import { times } from '../dist/times'

describe( 'times', () => {
  it( 'invokes a function a set amount of times and returns an array of results', () => {
    expect( times(3, String)).toEqual( ['0', '1', '2'] )
  })
})