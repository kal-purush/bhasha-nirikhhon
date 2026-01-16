import axios from 'axios'

export const state = () => ({
  products: [],
  cart: [],
  favorites: [],
})

export const mutations = {
  SET_PRODUCTS_TO_STATE(state, products) {
    state.products = products
  },
  SET_CART(state, product) {
    if (state.cart.length) {
      let isProductExists = false
      state.cart.map(function (item) {
        if (item.id === product.id) {
          isProductExists = true
        }
        return ''
      })
      if (!isProductExists) {
        state.cart.push(product)
      }
    } else {
      state.cart.push(product)
    }
  },
  REMOVE_FROM_CART(state, i) {
    state.cart.splice(i, 1)
  },
  SET_FAVORITES(state, product) {
    if (state.favorites.length) {
      let isProductExists = false
      state.favorites.map((item) => {
        if (item.id === product.id) {
          isProductExists = true
        }
        return ''
      })
      if (!isProductExists) {
        state.favorites.push(product)
      }
    } else {
      state.favorites.push(product)
    }
  },
  REMOVE_FROM_FAVORITES(state, i) {
    state.favorites.splice(i, 1)
  },
}
export const actions = {
  GET_PRODUCTS_FROM_API({ commit }) {
    return axios('http://localhost:3001/smartphones', {
      method: 'GET',
    }).then((products) => {
      commit('SET_PRODUCTS_TO_STATE', products.data)
      return products
    })
  },
  ADD_TO_CART({ commit }, product) {
    commit('SET_CART', product)
  },
  DELETE_FROM_CART({ commit }, i) {
    commit('REMOVE_FROM_CART', i)
  },
  ADD_TO_FAVORITES({ commit }, product) {
    commit('SET_FAVORITES', product)
  },
  DELETE_FROM_FAVORITES({ commit }, i) {
    commit('REMOVE_FROM_FAVORITES', i)
  },
}
export const getters = {
  PRODUCTS(state) {
    return state.products
  },
  CART(state) {
    return state.cart
  },
  FAVORITES(state) {
    return state.favorites
  },
}