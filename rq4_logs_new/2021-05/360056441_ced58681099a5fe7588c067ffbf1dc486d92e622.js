import Vue from 'vue'
import Vuex from 'vuex'
Vue.use(Vuex);
const state = {
    activeName: 'indexFrame'
}
const getters = {
    getActiveName: state => {
        return state.activeName;
    }
}
const mutations = {
    setActiveName (state, activeName) {
        state.activeName = activeName;
    }
}
const actions = {
    setActiveName (context, activeName) {
        context.commit('setActiveName', activeName);
    }
}
export default new Vuex.Store({
    state,
    getters,
    mutations,
    actions
});

















