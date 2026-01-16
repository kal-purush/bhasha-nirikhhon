import hasSplashScreenBeenShown from './index'
import { setHasSplashScreenBeenShown } from '../../actions'

describe('hasSplashScreenBeenShown', () => {
  describe('ReduxActionType.SetSplashScreenHasBeenShown', () => {
    it('should set the value to "false" when provide "false"', () => {
      const initialState: boolean = true
      const newState: boolean = hasSplashScreenBeenShown(initialState, setHasSplashScreenBeenShown(false))
      expect(newState).toBe(false)
    })

    it('should set the value to "true" when provide "true"', () => {
      const initialState: boolean = false
      const newState: boolean = hasSplashScreenBeenShown(initialState, setHasSplashScreenBeenShown(true))
      expect(newState).toBe(true)
    })
  })
})