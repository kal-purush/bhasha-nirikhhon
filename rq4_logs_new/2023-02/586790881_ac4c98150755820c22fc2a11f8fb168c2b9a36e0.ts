import {createStore} from "vuex";

const state = {
    url: '',
    isPlay: false
}
const getters = {
    url: state => state.url,
    isPlay: state => state.isPlay
}
const mutations = {
    SET_URL: (state,url) => state.url = url,
    SET_IS_PLAY: (state,isPlay) => state.isPlay = isPlay
}
export default createStore({
    state,
    getters,
    mutations
})