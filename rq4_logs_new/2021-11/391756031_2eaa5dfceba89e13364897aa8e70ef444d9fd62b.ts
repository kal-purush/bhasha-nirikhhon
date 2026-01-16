import axios from 'axios'
import Twitter from 'twitter-lite'
import { env } from '~/bin/dotenv'
import { request } from '~/test/util/supertest'

describe('/statuses', () => {
  // すべてのテストで利用する変数
  const mockServerOrigin = env.get('MOCK_SERVER_URL')

  // クエリパラメータの有効な範囲
  const rule = {
    screen_name: {
      min: 1,
      max: 15,
    },
    count: {
      min: 1,
      max: 200,
    },
  }

  // テスト向けにモックの初期化を行う
  let mockGet: jest.SpyInstance
  beforeEach(async () => {
    mockGet = jest.spyOn(Twitter.prototype, 'get')

    // モックサーバーに接続して、レスポンス内容を取得するように設定する
    const value = (
      await axios.get(`${mockServerOrigin}/statuses/user_timeline`)
    ).data

    mockGet.mockResolvedValue(value)
  })

  describe('/statuses/user_timeline', () => {
    test('正常なリクエスト（スクリーンネームのみ指定）', async () => {
      const parameter = new URLSearchParams({ screen_name: 'test' }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(200)
    })

    test('正常なリクエスト（すべてのオプションを指定）', async () => {
      const parameter = new URLSearchParams({
        screen_name: 'test',
        count: '30',
        max_id: '967574182522482687',
        include_rts: 'false',
      }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(200)
    })

    test('スクリーンネームの指定がない', async () => {
      const response = await request.get('/statuses/user_timeline')

      expect(response.statusCode).toEqual(400)
      expect(response.body.error).toBe('Bad Request')
    })

    test('スクリーンネームが空文字', async () => {
      const parameter = new URLSearchParams({ screen_name: '' }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(400)
      expect(response.body.error).toBe('Bad Request')
    })

    test('スクリーンネームが規定数をオーバーしている', async () => {
      const query = 'テスト'.repeat(rule.screen_name.max + 1)
      const parameter = new URLSearchParams({ screen_name: query }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(400)
      expect(response.body.error).toBe('Bad Request')
    })

    test.each([
      rule.count.min,
      /**
       * 最小〜最大の間の整数
       * @see https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Global_Objects/Math/random#getting_a_random_integer_between_two_values
       */
      Math.floor(
        Math.random() * (rule.count.max - rule.count.min) + rule.count.min,
      ),
      rule.count.max,
    ])('取得数が正常な範囲内（%p件）', async (count) => {
      const parameter = new URLSearchParams({
        screen_name: 'test',
        count: count.toString(),
      }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(200)
    })

    test('取得数が有効な値未満', async () => {
      const count = rule.count.min - 1
      const parameter = new URLSearchParams({
        screen_name: 'test',
        count: count.toString(),
      }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(400)
      expect(response.body.error).toBe('Bad Request')
    })

    test('取得数が有効な値以上', async () => {
      const count = rule.count.max + 1
      const parameter = new URLSearchParams({
        screen_name: 'test',
        count: count.toString(),
      }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(400)
      expect(response.body.error).toBe('Bad Request')
    })

    test('max_idが文字列で指定されている', async () => {
      const maxId = '967574182522482687'
      const parameter = new URLSearchParams({
        screen_name: 'test',
        max_id: maxId,
      }).toString()

      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(200)
    })

    test('max_idがオブジェクトとしてパースされる文字列で指定されている', async () => {
      const maxId = '967574182522482687'

      // 書式が特殊なため、URLSearchParamsは扱わず文字列で表現する
      const response = await request.get(
        `/statuses/user_timeline?screen_name=test&max_id[]=${maxId}&max_id[test]=${maxId}`,
      )

      expect(response.statusCode).toEqual(400)
      expect(response.body.error).toBe('Bad Request')
    })

    test('レート制限エラーが返却される', async () => {
      // エラーのレスポンス内容を設定する
      const value = (await axios.get(`${mockServerOrigin}/rate_limit`)).data
      mockGet.mockRejectedValue(value)

      const parameter = new URLSearchParams({ screen_name: 'test' }).toString()
      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(429)
    })

    test('Twitter APIの通信でエラーが返却される', async () => {
      // エラーのレスポンス内容を設定する
      mockGet.mockRejectedValue({
        errors: [
          {
            code: 0,
            message: 'error',
          },
        ],
      })

      const parameter = new URLSearchParams({ screen_name: 'test' }).toString()
      const response = await request.get(`/statuses/user_timeline?${parameter}`)

      expect(response.statusCode).toEqual(500)
    })
  })
})