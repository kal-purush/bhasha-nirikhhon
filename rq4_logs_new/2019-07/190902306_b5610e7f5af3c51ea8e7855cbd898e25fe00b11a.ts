import { createReducer } from 'typesafe-actions'

import { MyCompaniesQuery_myCompanies } from 'GraphqlTypes'
import { AppAction } from 'Config/rootAction'
import * as actions from './actions'

export interface CompanyState {
    myCompanies: MyCompaniesQuery_myCompanies[]
}

export const defaultCompanyState = {
    myCompanies: []
}

export const companyReducer = createReducer<CompanyState, AppAction>(
    defaultCompanyState
).handleAction(actions.getMyCompaniesAsync.success, (state, { payload }) => ({
    ...state,
    myCompanies: payload.data.myCompanies!
}))