import axios from 'axios'

const api = axios.create({
    baseURL: "https://6223ac143af069a0f9a7c310.mockapi.io/"
})

export default api;