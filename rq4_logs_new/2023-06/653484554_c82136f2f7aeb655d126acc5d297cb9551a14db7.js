import axios from 'axios'

const axiosIns = axios.create({
  baseURL: 'http://localhost', // timeout: 1000,
  headers: {
    'Accept': 'application/json',
  },
})

export default axiosIns