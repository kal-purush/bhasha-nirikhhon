import axios from 'axios'

const instance = axios.create({
  baseURL: 'https://vuejs-axios-7909e.firebaseio.com'
})

instance.defaults.headers.common['SOMETHING'] = 'something'