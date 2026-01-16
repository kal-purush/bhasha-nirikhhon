import * as snapshotDiff from 'snapshot-diff'
import * as reducers from '.'
import { initialState } from './reducers'

describe('VisibilityFilter reducers test', () => {
  const {
    initialState,
    visibilityFilter,
    VisibilityFilters,
    setVisibilityFilter
  } = reducers

  test('should return initial state', () => {
    expect(visibilityFilter(initialState, {} as any)).toBe(
      VisibilityFilters.SHOW_ALL
    )
  })

  test('SHOW_ALL', () => {
    expect(
      snapshotDiff(
        initialState,
        visibilityFilter(
          initialState,
          setVisibilityFilter(VisibilityFilters.SHOW_ALL)
        )
      )
    ).toMatchSnapshot()
  })

  test('SHOW_ACTIVE', () => {
    expect(
      snapshotDiff(
        initialState,
        visibilityFilter(
          initialState,
          setVisibilityFilter(VisibilityFilters.SHOW_ACTIVE)
        )
      )
    ).toMatchSnapshot()
  })

  test('SHOW_COMPLETED', () => {
    expect(
      snapshotDiff(
        initialState,
        visibilityFilter(
          initialState,
          setVisibilityFilter(VisibilityFilters.SHOW_COMPLETED)
        )
      )
    ).toMatchSnapshot()
  })
})