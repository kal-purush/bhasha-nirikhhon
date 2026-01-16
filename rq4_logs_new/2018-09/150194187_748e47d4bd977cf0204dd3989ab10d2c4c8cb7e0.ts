import fetch from 'cross-fetch'
import { createProblemError, DEFAULT_TYPE as DEFAULT_ERROR_TYPE } from './helpers'

type HTTP_METHOD = 'GET' | 'HEAD' | 'POST' | 'PUT' | 'DELETE' | 'CONNECT' | 'OPTIONS' | 'TRACE' | 'PATCH'

export class Base {
  private static async request(method: HTTP_METHOD, url: string, options?: RequestInit): Promise<any> {
    const res = await fetch(url, { ...options, method, mode: 'cors' }).catch((e: Error) => { throw e })

    const resBody = await res.json().catch((e: Error) => {
        throw createProblemError('Internal Server Error', 500, e, DEFAULT_ERROR_TYPE, { detail: 'JSON parse error' })
      })

      if (!res.ok) {
        throw createProblemError(res.statusText, res.status, null, DEFAULT_ERROR_TYPE, resBody)
      }

    return resBody
  }

  protected static async get(url: string, options?: RequestInit): Promise<any> {
    return await this.request('GET', url, options)
  }

  protected static async post(url: string, options?: RequestInit): Promise<any> {
    return await this.request('POST', url, options)
  }

  protected static async put(url: string, options?: RequestInit): Promise<any> {
    return await this.request('PUT', url, options)
  }

  protected static async patch(url: string, options?: RequestInit): Promise<any> {
    return await this.request('PATCH', url, options)
  }

  protected static async delete(url: string, options?: RequestInit): Promise<any> {
    return await this.request('DELETE', url, options)
  }
}