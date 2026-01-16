import { act, renderHook, waitFor } from '@testing-library/react'
import axios from 'axios'

import { useApi } from './useApi'

jest.mock('axios')
const mockedAxios = jest.mocked(axios)
const url = 'http://localhost:3000/test'

describe('useApi', () => {
  beforeEach(() => {
    mockedAxios.get.mockReset()
    mockedAxios.mockReset()
  })

  describe('GET', () => {
    it('データを取得できる', async () => {
      mockedAxios.get.mockResolvedValueOnce({ data: { message: 'Get data' } })
      const { result } = renderHook(() => useApi(url))
      await waitFor(() => expect(result.current.data).not.toBeNull())
      expect(result.current.data).toEqual({ message: 'Get data' })
      expect(result.current.error).toBeNull()
    })

    it('GETリクエストでエラーが発生する', async () => {
      mockedAxios.get.mockRejectedValueOnce(new Error('Something went wrong'))
      const { result } = renderHook(() => useApi(url))
      await waitFor(() =>
        expect(result.current.error).toBe('エラーが発生しました'),
      )
    })
  })

  describe('POST', () => {
    it('POST処理が実行される', async () => {
      const postData = { message: 'Post data' }
      mockedAxios.mockResolvedValueOnce({
        url: url,
        method: 'post',
        data: postData,
        headers: {
          'Content-Type': 'application/json',
        },
      })
      const { result } = renderHook(() => useApi(url))
      let response
      await act(async () => {
        response = await result.current.postData(postData)
      })
      expect(response!.data).toEqual(postData)
    })

    it('POSTリクエストでエラーが発生する', async () => {
      mockedAxios.mockRejectedValueOnce(new Error('Error message'))
      const { result } = renderHook(() => useApi(url))
      await act(async () => {
        await result.current.postData({})
      })
      expect(result.current.error).toBe('エラーが発生しました')
    })
  })
})