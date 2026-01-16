/**
 * Check URL against Regular Expression
 *
 * @param string url
 * @return string|boolean
 */
const parseYoutubeId = (url: string): string | boolean => {
  const regexp = new RegExp(
    '(?:youtube(?:-nocookie)?.com/(?:[^/]+/.+/|(?:v|e(?:mbed)?)/|.*[?&]v=)|youtu.be/)([^"&?/s]{11})',
    'i'
  )
  const matches: RegExpMatchArray | null = url.match(regexp)

  return matches ? matches[1] : false
}

export default parseYoutubeId