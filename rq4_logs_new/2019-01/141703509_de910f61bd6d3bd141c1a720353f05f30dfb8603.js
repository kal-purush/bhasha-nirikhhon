
const state = {
  product: 'produce1'
}

const getters = {
  getProduct (state) {
    return state.product;
  } 
}

const mutations = {
  setProduct (state, value) {
    state.product = value;
  } 
}

const actions = {
  modifyProduct(context,params){
    context.commit("setProduct",params);
  }
}

export default {
  namespaced: true,
  state,
  getters,
  mutations,
  actions
}
