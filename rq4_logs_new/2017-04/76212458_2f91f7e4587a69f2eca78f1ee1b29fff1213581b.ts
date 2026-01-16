import { paymentInfo as apiClient } from 'src/infrastructure/api_client'

import { GET_CUSTOMER_ID, GET_IS_LOGGED_IN }
  from 'src/infrastructure/store/auth_types'

import { GET_PAYMENTS } from './getters'
import { SET_PAYMENTS } from './mutations'

export const FETCH_PAYMENTS = 'customer_info/FETCH_PAYMENTS'

const actions = {
  async[FETCH_PAYMENTS]({ commit, getters }, forceFetch = false) {
    const isLoggedIn = getters[GET_IS_LOGGED_IN]
    if (isLoggedIn) {
      if (forceFetch || !getters[GET_PAYMENTS]) {
        const id = getters[GET_CUSTOMER_ID]
        const payments = await apiClient.getPayments(id)
        commit(SET_PAYMENTS, payments)
      }
    }
  }
}

export default actions