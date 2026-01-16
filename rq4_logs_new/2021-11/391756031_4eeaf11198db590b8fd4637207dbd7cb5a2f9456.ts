/**
 * 型に関する検証処理（アサーション）を行う処理
 * @see https://www.typescriptlang.org/docs/handbook/release-notes/typescript-3-7.html#assertion-functions
 */

import Twitter from 'twitter-lite'

/**
 * 独自処理のアサーションで利用される型
 * アサーションでは明示的に型を宣言する必要があるため、予め定義している
 *
 * @note 型を宣言していなかった場合に出てくるエラー
 * > アサーションでは、呼び出し先のすべての名前が明示的な型の注釈で宣言されている必要があります。ts(2775)
 * > assert.ts(28, 7): 'assertIsString' には、明示的な型の注釈が必要です。
 */
type assertGenericsType<T> = (value: any) => asserts value is T

/**
 * ジェネリクスを利用したアサーション処理
 * @param value 検証対象となる値
 * @param validator 検証処理
 * @throws {@link TypeError} 検証の結果、適切でない値と判断された場合
 * @see https://github.com/microsoft/TypeScript/issues/41047#issuecomment-706761663 参考にしたコード
 */
const assertGenerics: <T>(
  value: any,
  validator: (value: any) => boolean,
) => asserts value is T = (value, validator) => {
  if (validator(value) === false) {
    throw new TypeError('assertion failed.')
  }
}

/**
 * 文字列型であることを検証する
 * @param value 検証したい値
 */
const assertIsTwitterClient: assertGenericsType<Twitter> = (value: any) =>
  assertGenerics<Twitter>(value, (value) => value instanceof Twitter)

export { assertIsTwitterClient }