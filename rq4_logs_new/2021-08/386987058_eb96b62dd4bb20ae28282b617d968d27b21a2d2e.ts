import useSWR from 'swr'
import { Fetcher, SWRResponse } from 'swr/dist/types'

import { PERSONAS_APP } from 'constants/locations'

import { RequestOptions, RequestOption, OptionsType } from 'types'

const buildRequestUrl = (url: string, RequestParams?: RequestOptions) => {
  if (!RequestParams) {
    return url
  }
  const queryString = Object.keys(RequestParams)
    .map(key => `${key}=${RequestParams[key as RequestOption]}`)
    .join('&')
  return `${url}?${queryString}`
}

const getDefaultFetcher = (url: string) => () =>
  fetch(url).then(res => res.json())

export const usePersonasAPI = ({
  url,
  fetcher,
  options,
  requestParams,
}: {
  url: string
  fetcher?: Fetcher<any>
  options?: OptionsType
  requestParams?: RequestOptions
}): SWRResponse<any, any> => {
  const apiUrl = `${PERSONAS_APP}api/${buildRequestUrl(url, requestParams)}`
  const apiFetcher = (fetcher || getDefaultFetcher(apiUrl)) as Fetcher<any>
  return useSWR(apiUrl, apiFetcher, options)
}