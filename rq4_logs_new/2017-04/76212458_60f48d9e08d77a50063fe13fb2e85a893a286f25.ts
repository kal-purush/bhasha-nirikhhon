import Vue from 'vue'
import Checkout from 'src/router_views/Checkout'
import store from 'src/infrastructure/store'
import { GET_IS_LOGGED_IN } from 'src/infrastructure/store/auth_types'
import { SHOW_SIGN_IN } from 'src/infrastructure/store/modal_dialogs_types'

export default {
  name: 'checkout',
  path: '/checkout',
  component: Checkout,

  // a customer must be logged in to perform checkout
  // if not, display signup/login.
  // To make it simple, show cart regardless of the result of signup/login
  beforeEnter: (to, from, next) => {
    let isLoggedIn = store.getters[GET_IS_LOGGED_IN]
    if (isLoggedIn) {
      next()
    } else {
      store.dispatch(SHOW_SIGN_IN)
      next('/cart')
    }
  }
}