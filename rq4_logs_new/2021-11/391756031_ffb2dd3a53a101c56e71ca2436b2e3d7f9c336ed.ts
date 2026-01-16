/**
 * エラーメッセージに関する処理
 */

import { errors, messages } from '~/preferences/errorMessage'

/**
 * HTTPステータスコードから、エラーメッセージを算出する
 * @param statusCode ステータスコード
 * @returns エラーメッセージ
 */
const getFromStatusCode = (statusCode: number) => {
  if (statusCode in errors) {
    /**
     * オブジェクトのプロパティにブラケット記法でアクセスできるように、アサーション処理を行う
     * 直接アクセスしようとすると以下のエラーが発生するため、それを回避している
     * TypeScript上の検証を無視しているため、事前にif文等で値を検証することを忘れないよう注意する
     * > Element implicitly has an 'any' type because type 'typeof *****' has no index signature.
     */
    const key = statusCode as keyof typeof errors
    return errors[key]
  }

  return messages.default
}

export { getFromStatusCode }