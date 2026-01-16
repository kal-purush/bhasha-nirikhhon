/**
 * 独自のエラークラス
 */

/**
 * 独自のエラークラス
 * エラーメッセージの他に、独自のデータを扱う
 *
 * @see https://developer.mozilla.org/ja/docs/Web/JavaScript/Reference/Global_Objects/Error#custom_error_types
 */
class CustomError extends Error {
  data?: any

  constructor(...params: any) {
    // Pass remaining arguments (including vendor specific ones) to parent constructor
    super(...params)

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, CustomError)
    }

    this.name = params.name ?? 'CustomError'

    // Custom debugging information
    this.data = params.data
  }
}

/**
 * メディアのダウンロードで発生する例外
 */
class MediaDownloadError extends CustomError {
  constructor(...params: any) {
    params.name = 'MediaDownloadError'
    super(...params)

    // Maintains proper stack trace for where our error was thrown (only available on V8)
    if (CustomError.captureStackTrace) {
      CustomError.captureStackTrace(this, MediaDownloadError)
    }
  }
}

export { CustomError, MediaDownloadError }