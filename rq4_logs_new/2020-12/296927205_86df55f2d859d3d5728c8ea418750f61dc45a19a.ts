import {
  initialSettingsState,
  settingsActions,
  settingsReducer,
} from './settings.slice'

describe('settings reducer', () => {
  it('should handle initial state', () => {
    const expected = initialSettingsState

    expect(settingsReducer(undefined, { type: '' })).toEqual(expected)
  })

  it('should handle open', () => {
    const expected = { ...initialSettingsState, open: true }

    expect(settingsReducer(undefined, settingsActions.open())).toEqual(expected)
  })

  it('should handle close', () => {
    const expected = { ...initialSettingsState, open: false }

    expect(settingsReducer(undefined, settingsActions.close())).toEqual(
      expected
    )
  })

  it('should handle setChampionVendor', () => {
    const expected = {
      ...initialSettingsState,
      championVendor: 'mock-champion-vendor',
    }

    expect(
      settingsReducer(
        undefined,
        settingsActions.setChampionVendor('mock-champion-vendor')
      )
    ).toEqual(expected)
  })

  it('should handle setSummonerVendor', () => {
    const expected = {
      ...initialSettingsState,
      summonerVendor: 'mock-summoner-vendor',
    }

    expect(
      settingsReducer(
        undefined,
        settingsActions.setSummonerVendor('mock-summoner-vendor')
      )
    ).toEqual(expected)
  })
})