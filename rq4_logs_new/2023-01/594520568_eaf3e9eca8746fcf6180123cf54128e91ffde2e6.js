export function getLocalStorage (key) {
  const data = window.localStorage.getItem(key)
  return data ? JSON.parse(data) : null
}

export function setLocalStorage (key, data) {
  window.localStorage.setItem(key, JSON.stringify(data))
}