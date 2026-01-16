/*
  About Store Unit Test.
*/

import { store } from '@/ducks'
import { aboutMeToggle } from '@/ducks/AboutMe'

beforeAll(() => {})
beforeEach(() => {})

describe('About Me Store Unit Test', () => {
  it('About Me Open', () => {
    store.dispatch(aboutMeToggle({ isOpen: true }))
    expect(store.getState().aboutMeStore.isOpen).toBeTruthy
  })
  it('About Me Close', () => {
    store.dispatch(aboutMeToggle({ isOpen: false }))
    expect(store.getState().aboutMeStore.isOpen).toBeFalsy
  })
})

afterEach(() => {})
afterAll(() => {})