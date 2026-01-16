export const formatDate = (date: string) => {
  const dateObj = new Date(date)
  const defaultOptions = {
    timeZone: 'Europe/Amsterdam',
  }
  const dateString = new Intl.DateTimeFormat('nl-NL', {
    ...defaultOptions,
    weekday: 'long',
    day: 'numeric',
    month: 'long',
  }).format(dateObj)
  const timeString = new Intl.DateTimeFormat('nl-NL', {
    ...defaultOptions,
    hour: 'numeric',
    minute: 'numeric',
  }).format(dateObj)
  return `${capitalizeFirstLetter(dateString)} — ${timeString}`
}

const capitalizeFirstLetter = (string: string) => {
  return string.charAt(0).toUpperCase() + string.slice(1)
}