/**
 * 環境変数に関する設定
 * dotenvは標準の初期化処理では値が未定義であるかどうかを判別しないため、追加で処理を実施している
 */
import path from 'path'
import dotenv from 'dotenv'

/**
 * .envファイルから環境変数を読み込む
 * 以下の処理を行う
 * - dotenvの初期設定
 * - 未定義の環境変数がないか検証する
 * @throws {@link TypeError} dotenvの初期設定に失敗した場合
 * @throws {@link TypeError} 未定義の環境変数が存在する場合
 */
const config = () => {
  // .envファイルを読み込む
  const option = {
    path: path.resolve(__dirname, '../../.env'),
  }
  const parsedConfig = dotenv.config(option).parsed
  // ファイルが読み込めなかった場合、エラーを発生させる
  if (parsedConfig === undefined) {
    throw new TypeError('.envファイルの値が取得できませんでした')
  }

  // 未定義（パース処理によりすべて文字列となっているため、空文字を指す）の値がないか検証する
  // 独自の処理を行う（該当のキー名を保持しておいて、エラー文中に表示したい）ため、検証向けの関数（Object.values()->Array.includes()等）は使わずに、プロパティを列挙しながら検証する
  const undefinedKeys: string[] = []
  for (const [key, value] of Object.entries(parsedConfig)) {
    if (value !== '') {
      continue
    }

    undefinedKeys.push(key)
  }

  // 未定義の環境変数（=空文字の要素）が存在する場合、エラーを発生させる
  const hasUndefined = undefinedKeys.length > 0
  if (hasUndefined) {
    const targetKey = undefinedKeys.join('\n')
    const message = `
以下の環境変数が定義されていません

${targetKey}
`
    throw new TypeError(message)
  }
}

/**
 * 指定された環境変数の値を取得する
 * @param name 環境変数のキー名
 * @returns 指定指定された環境変数の値
 * @throws {@link TypeError} 指定の環境変数が未定義だった場合
 */
const get = (name: string): string => {
  const value = process.env[name]

  if (value === undefined) {
    throw new TypeError(`環境変数 ${name} が定義されていません`)
  }

  return value
}

const env = {
  config,
  get,
}

export { env }