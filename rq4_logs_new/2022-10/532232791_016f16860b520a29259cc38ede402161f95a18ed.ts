import axios from 'axios'

const provinceApi = {
  getProvince(): Promise<any> {
    const result = axios.get('https://provinces.open-api.vn/api/?depth=3')
    return result
  },
}

export default provinceApi