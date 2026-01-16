import axios from 'axios'
export const state = () => ({
    role: null,
    token: ''
})

export const getters = {
}

export const mutations = {
    setState(state,{key,payload}){
        state[key] = payload
    }
}

export const actions = {
    async login(_, {form}){
        let res = await axios.post('http://localhost:3000/auth', form)
        // harakat muvaffaqiyatli bo'lib token olgandan leyin uni parse qilib role ni bilib olamiz
        // token va rolni localstorage va kerak bolsa storeda saqlaymiz
        return res
    },
    logout(){
        let res = axios.post('http://localhost:3000/logout')
        // localstorage va store malumotlarni tozalab yuboramiz
        return res
    },
}