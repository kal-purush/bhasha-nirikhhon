import { of, Subject } from 'rxjs'
import { ActionsObservable, StateObservable } from 'redux-observable'
import * as TypeMoq from 'typemoq'

import { RoutingService } from 'Common/Services/routingService'
import { CompanyService } from '../service'
import { companyEpicFactory } from '../epic'
import * as actions from '../actions'
import {
    createCompanyVariablesMock,
    apolloCreateCompanyMock,
    companyMock
} from './mocks'

describe('Casting Epic', () => {
    const mockState = new StateObservable(new Subject(), {})
    let mockCompanyService: TypeMoq.IMock<CompanyService>
    let mockRoutingService: TypeMoq.IMock<RoutingService>

    beforeEach(() => {
        mockCompanyService = TypeMoq.Mock.ofType<CompanyService>()
        mockRoutingService = TypeMoq.Mock.ofType<RoutingService>()
    })

    it('should create company', done => {
        mockCompanyService
            .setup(x =>
                x.createCompany(
                    TypeMoq.It.isObjectWith(createCompanyVariablesMock)
                )
            )
            .returns(() => Promise.resolve(apolloCreateCompanyMock))
            .verifiable()

        const companyEpicFactoryInstance = companyEpicFactory(
            mockCompanyService.object,
            mockRoutingService.object
        )
        const action$ = of(
            actions.createCompanyAsync.request(createCompanyVariablesMock)
        )

        companyEpicFactoryInstance(
            new ActionsObservable(action$),
            mockState,
            null
        ).subscribe(res => {
            expect(res).toEqual(actions.createCompanyAsync.success(companyMock))
            mockCompanyService.verifyAll()

            done()
        })
    })
})