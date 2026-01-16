import { RequestOptions } from 'https'

export default async (endpoint: string, config: RequestOptions) => {
  const fetchConfig = {
    ...config,
    headers: {
      'Content-type': 'application/json',
      ...config.headers,
    },
  }

  const result = await fetch(endpoint, fetchConfig)

  const contentType = result.headers.get('content-type')

  const response = !contentType?.includes('application/json')
    ? result.text()
    : result.json()

  if (!result.ok) {
    throw new Error(await response)
  }

  return response
}