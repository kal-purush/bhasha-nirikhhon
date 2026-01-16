import axios from 'axios'

const api = axios.create({
  baseURL: 'https://fakestoreapi.com',
  headers: {
    "Content-type": "application/json"
  }
})

const ApiClient = {
  get: async(url:string) => {
    const response = await api.get(url)
    return response.data
  },

  post: async(url:string, body:any) => {
    const response = await api.post(url, body)
    return response.data
  },

  put: async(url:string, body:any) => {
    const response = await api.put(url, body)
    return response.data
  },

  delete: async(url:string, ) => {
    const response = await api.delete(url);
    return response.data
  }
}

export default ApiClient