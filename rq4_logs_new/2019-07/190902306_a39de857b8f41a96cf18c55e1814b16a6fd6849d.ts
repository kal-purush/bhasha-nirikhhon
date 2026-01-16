import { StateObservable, ActionsObservable } from 'redux-observable'
import { Subject, of } from 'rxjs'
import * as TypeMoq from 'typemoq'

import { RoutingService } from 'Common/Services/routingService'
import { AuthService } from '../service'
import { authEpicFactory } from '../epic'
import * as actions from '../actions'
import {
    mockLoginVariables,
    mockLoginResponse,
    mockRegisterVariables,
    mockRegisterResponse
} from './mocks'

describe('Auth Epic', () => {
    const mockState = new StateObservable(new Subject(), {})
    let mockAuthService: TypeMoq.IMock<AuthService>
    let mockRoutingService: TypeMoq.IMock<RoutingService>

    beforeEach(() => {
        mockAuthService = TypeMoq.Mock.ofType<AuthService>()
        mockRoutingService = TypeMoq.Mock.ofType<RoutingService>()
    })

    it('should handle login', done => {
        mockAuthService
            .setup(x => x.login(TypeMoq.It.isObjectWith(mockLoginVariables)))
            .returns(() => Promise.resolve(mockLoginResponse))
            .verifiable()

        mockRoutingService
            .setup(x => x.push(TypeMoq.It.isValue('/castings')))
            .verifiable()

        const authEpicFactoryInstance = authEpicFactory(
            mockAuthService.object,
            mockRoutingService.object
        )

        const action$ = of(actions.loginAsync.request(mockLoginVariables))

        authEpicFactoryInstance(
            new ActionsObservable(action$),
            mockState,
            null
        ).subscribe(res => {
            expect(res).toEqual(
                actions.loginAsync.success(mockLoginResponse.data.login)
            )
            mockAuthService.verifyAll()
            mockRoutingService.verifyAll()

            done()
        })
    })

    it('should handle register', done => {
        mockAuthService
            .setup(x =>
                x.register(TypeMoq.It.isObjectWith(mockRegisterVariables))
            )
            .returns(() => Promise.resolve(mockRegisterResponse))
            .verifiable()

        const authEpicFactoryInstance = authEpicFactory(
            mockAuthService.object,
            mockRoutingService.object
        )

        const action$ = of(actions.registerAsync.request(mockRegisterVariables))

        authEpicFactoryInstance(
            new ActionsObservable(action$),
            mockState,
            null
        ).subscribe(res => {
            expect(res).toEqual(actions.registerAsync.success(true))
            mockAuthService.verifyAll()

            done()
        })
    })
})