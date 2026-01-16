import createStore from '../store/createStore'
import { reducer as counterReducer } from './counter'
import { reducer as settingReducer } from './setting'

const store = createStore()

test('should return root reducer', () => {
  expect(store.getState().counter).toEqual(
    counterReducer(undefined, { type: undefined as any })
  )

  expect(store.getState().setting).toEqual(
    settingReducer(undefined, { type: undefined as any, payload: '' })
  )
})