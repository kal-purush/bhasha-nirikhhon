import repository from "../../api/repository"
export const namespaced = true
export const state = {
//   artists: null,
//   charities: null
}
export const getters = {

}
export const mutations = {
   
}
export const actions = {
    async getAllArtists({ commit }) {
        try {
            const { data } = await repository.getAllArtists()
            // console.log(data)
            return data
        } catch (e) {
            throw e
        }
    },
    async getAllCharities({ commit }) {
        try {
            const { data } = await repository.getAllCharities()
            // console.log(data)
            return data
        } catch (e) {
            throw e
        }
    },
    async getArtistDetails({ commit }, payload) {
        try {
            const { data } = await repository.getArtistDetails(payload)
            // console.log(data)
            return data
        } catch (e) {
            throw e
        }
    }
}