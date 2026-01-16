import axios from 'axios'

export default () => {
  return axios.create({
    baseURL: process.env.VUE_APP_LAMBDA_URL,
    headers: {
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    }
  })
}