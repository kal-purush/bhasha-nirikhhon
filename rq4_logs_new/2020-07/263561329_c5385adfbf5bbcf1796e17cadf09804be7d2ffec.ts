export const removeNonAscii = (s: string): string => s.replace(/[^\x00-\x7F]/g, '')

export const sortByStringField = <T>(fieldSelector: (t: T) => string, map: (s: string) => string = (s) => s) => (
  self: T,
  other: T
) =>
  map(fieldSelector(self))
    .trim()
    .localeCompare(map(fieldSelector(other)).trim())