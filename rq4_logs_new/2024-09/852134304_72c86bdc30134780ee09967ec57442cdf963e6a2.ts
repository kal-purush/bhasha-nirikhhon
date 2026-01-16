import axios from 'axios'

const AXIOS = axios.create({
  baseURL: `${import.meta.env.VITE_API}/api/v1`,
})

export default AXIOS