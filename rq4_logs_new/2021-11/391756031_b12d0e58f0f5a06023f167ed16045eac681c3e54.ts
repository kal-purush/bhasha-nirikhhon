import { getFromStatusCode } from '~/modules/errorMessage'
import { errors, messages } from '~/preferences/errorMessage'

describe('エラーメッセージに関する処理', () => {
  test.each(Object.keys(errors))(
    '規定のステータスコードを指定すると、対応したエラーメッセージが出力される（%p）',
    (statusCode) => {
      const code = parseInt(statusCode)
      const message = getFromStatusCode(code)

      switch (code) {
        case 400:
          expect(message).toBe(messages.badRequest)
          break
        case 401:
          expect(message).toBe(messages.unauthorized)
          break
        case 404:
          expect(message).toBe(messages.notFound)
          break
        case 429:
          expect(message).toBe(messages.tooManyRequests)
          break
      }
    },
  )

  test('規定以外のステータスコードを指定すると、デフォルトのエラーメッセージが出力される', () => {
    expect(getFromStatusCode(999)).toBe(messages.default)
  })
})