import { castingsReducer, defaultCastingState } from '../reducer'
import { ApolloCastingsMock, CastingMock } from './mocks'
import * as actions from '../actions'

describe('Casting Reducer', () => {
    it('should create default state', () => {
        expect(castingsReducer(undefined, {})).toEqual(defaultCastingState)
    })

    it('should save fetched castings', () => {
        const state = castingsReducer(
            defaultCastingState,
            actions.getCastingsSucceed(ApolloCastingsMock)
        )

        expect(state).toEqual({ castings: [CastingMock] })
    })

    it('should save new casting to the store', () => {
        const state = castingsReducer(
            defaultCastingState,
            actions.createCastingSucceed(CastingMock)
        )

        expect(state).toEqual({
            castings: [CastingMock]
        })
    })
})