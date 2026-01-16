export default function pdfLinkAnchorFor (element: HTMLElement): HTMLElement | null {
  const matchingTagNames = ['a']
  const matchingAttribute = 'href'
  const matchingUrlRegex = /.*\.pdf$/

  const matchingCandidates =
    matchingTagNames
      .map(tagName => { return element.closest(tagName) })
      .filter(element => element != null) as HTMLElement[]

  const matches = matchingCandidates
    .filter(element => {
      const url = element.getAttribute(matchingAttribute)
      return (url != null) && matchingUrlRegex.test(url)
    })

  if (matches.length === 0) return null

  return matches[0]
}