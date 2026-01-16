import { of, Subject } from 'rxjs'
import { ActionsObservable, StateObservable } from 'redux-observable'
import * as TypeMoq from 'typemoq'

import { CastingService } from '../service'
import { castingEpicFactory } from '../epic'
import * as actions from '../actions'
import {
    ApolloCastingsMock,
    ApolloCreateCastingMock,
    CreateCastingVariables,
    CastingMock
} from './mocks'

enum MockMethods {
    GET = 'getCastings',
    CREATE = 'createCasting'
}

describe('Casting Epic', () => {
    const mockState = new StateObservable(new Subject(), {})
    let mockCastingService: TypeMoq.IMock<CastingService>

    beforeEach(() => {
        mockCastingService = TypeMoq.Mock.ofType<CastingService>()
    })

    it('should fetch casting', done => {
        mockCastingService
            .setup(x => x.getCastings())
            .returns(() => Promise.resolve(ApolloCastingsMock))
            .verifiable()

        const castingEpicFactoryInstance = castingEpicFactory(
            mockCastingService.object
        )
        const action$ = of(actions.getCastingsRequest())

        castingEpicFactoryInstance(
            new ActionsObservable(action$),
            mockState,
            null
        ).subscribe(res => {
            expect(res).toEqual(actions.getCastingsSucceed(ApolloCastingsMock))
            mockCastingService.verifyAll()

            done()
        })
    })

    it('should create casting', done => {
        mockCastingService
            .setup(x =>
                x.createCasting(TypeMoq.It.isObjectWith(CreateCastingVariables))
            )
            .returns(() => Promise.resolve(ApolloCreateCastingMock))

        const castingEpicFactoryInstance = castingEpicFactory(
            mockCastingService.object
        )
        const action$ = of(actions.createCastingRequest(CreateCastingVariables))

        castingEpicFactoryInstance(
            new ActionsObservable(action$),
            mockState,
            null
        ).subscribe(res => {
            expect(res).toEqual(actions.createCastingSucceed(CastingMock))
            mockCastingService.verifyAll()

            done()
        })
    })
})