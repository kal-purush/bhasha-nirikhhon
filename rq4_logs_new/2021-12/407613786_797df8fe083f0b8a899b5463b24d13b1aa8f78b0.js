import axios from 'axios';


export const state = ()=>{
    buyers:[]
}

export const mutations = {
    listOfBuyers(state,payload){
        state.buyers = payload
    }
}
export const actions = {
    async fetchBuyers({commit}){
        const data = (await (axios.get('http://0.0.0.0:8000/api/buyers/'))).data
        commit('listOfBuyers',data)
    }
}
export const getters = {}