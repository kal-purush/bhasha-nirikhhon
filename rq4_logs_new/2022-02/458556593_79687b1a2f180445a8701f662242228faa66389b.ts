export const isEmpty = (value: any): boolean => {
  if (typeof value === 'string') {
    value = value.trim()
  }

  return (
    value === null ||
    value === undefined ||
    value === false ||
    (Array.isArray(value) && value.length === 0) ||
    value === '' ||
    (value.constructor === Object && Object.keys(value).length === 0)
  )
}

export const stringToColor = (string: string) => {
  let hash = 0
  let i

  /* eslint-disable no-bitwise */
  for (i = 0; i < string.length; i += 1) {
    hash = string.charCodeAt(i) + ((hash << 5) - hash)
  }

  let color = '#'

  for (i = 0; i < 3; i += 1) {
    const value = (hash >> (i * 8)) & 0xff
    color += `00${value.toString(16)}`.substr(-2)
  }

  return color
}