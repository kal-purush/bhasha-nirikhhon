import { useCallback, useEffect, useState } from 'react'

export const useReadLocalStorage = <T>(
  key: string,
  immediate = true,
): [T | undefined, () => void] => {
  const [storedValue, setValue] = useState<T>()

  const getValue = useCallback(() => {
    try {
      const item = window.localStorage.getItem(key)
      return item ? (JSON.parse(item) as T) : null
    } catch (error) {
      console.warn(`Error reading localStorage key “${key}”:`, error)
      return undefined
    }
  }, [key])

  const readStoredValue = useCallback(() => {
    const value = getValue()
    if (value) setValue(value)
  }, [getValue])

  useEffect(() => {
    if (immediate) {
      readStoredValue()
    }
  }, [immediate, readStoredValue])

  return [storedValue, readStoredValue]
}