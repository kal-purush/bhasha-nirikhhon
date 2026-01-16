import axios, { AxiosRequestConfig, Method } from 'axios'

export interface CONFIG {
  method: Method
  endpoint: string
  params?: any
  headers?: any
  data?: any
}

const HOST = process.env.CLIENT_URL || 'http://localhost:5000'

const axiosRequestHandler = async (obj: CONFIG) => {
  const config: AxiosRequestConfig = {
    method: obj.method as Method,
    url: `${HOST || ''}/${obj.endpoint}`,
    withCredentials: false,
    headers: {
      ...obj.headers,
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': HOST,
      'Access-Control-Allow-Methods': 'GET,PUT,POST,DELETE,'
    },
    params: obj.params,
    data: obj.data
  }

  try {
    const response = await axios(config)
    if (response.status === 200 || response.status === 201) {
      return [true, response.data]
    } else {
      return [false, response]
    }
  } catch (error) {
    return [false, error]
  }
}

export default axiosRequestHandler
