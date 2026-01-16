/**
 * ツイートに含まれるユーザー情報に関する設定
 */

/**
 * プロフィール画像のURL末尾に付与される縮小フィルタの文字列
 * これを取り除くことで、オリジナル画像へのアクセスが可能になる
 */
const profileImageUrlSuffix = {
  jpg: {
    suffix: '_normal.jpg',
    replacement: '.jpg',
  },
  gif: {
    suffix: '_normal.gif',
    replacement: '.gif',
  },
}

/**
 * 文字列について、**末尾の文字**を対象に置換処理を行う
 * @param string 置換したい文字列
 * @param pattern 置き換え対象となる文字列
 * @param replacement 置き換える文字列
 * @returns 置換後の文字列
 */
const replaceSuffix = (
  string: string,
  pattern: string,
  replacement: string,
) => {
  return string.replace(new RegExp(`${pattern}$`), replacement)
}

/**
 * アイコン画像のURLから一部の文字列を取り除いて、オリジナル画像のURLを取得する
 * @param url アイコン画像のURL
 * @returns オリジナル画像のURL
 */
const getOriginalProfileImageUrl = (url: string) => {
  // JPEG画像
  if (url.endsWith(profileImageUrlSuffix.jpg.suffix)) {
    return replaceSuffix(
      url,
      profileImageUrlSuffix.jpg.suffix,
      profileImageUrlSuffix.jpg.replacement,
    )
  }

  // GIF画像
  return replaceSuffix(
    url,
    profileImageUrlSuffix.gif.suffix,
    profileImageUrlSuffix.gif.replacement,
  )
}

export { getOriginalProfileImageUrl }