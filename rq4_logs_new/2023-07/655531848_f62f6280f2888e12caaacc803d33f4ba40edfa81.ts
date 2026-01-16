import { Dispatch, SetStateAction, useCallback, useState } from 'react'

type SetValue<T> = Dispatch<SetStateAction<T | undefined>>

export const useLocalStorage = <T>(
  key: string,
  initialValue?: T,
): [T | undefined, SetValue<T>] => {
  const readValue = useCallback((): T | undefined => {
    try {
      const item = window.localStorage.getItem(key)
      console.log(item)
      return item ? (parseJSON(item) as T) : initialValue
    } catch (error) {
      console.warn(`Error reading localStorage key “${key}”:`, error)
      return initialValue
    }
  }, [initialValue, key])

  const [storedValue, setStoredValue] = useState<T | undefined>(readValue)

  const setValue: SetValue<T> = useCallback(
    value => {
      try {
        const newValue = value instanceof Function ? value(storedValue) : value

        window.localStorage.setItem(key, JSON.stringify(newValue))

        setStoredValue(newValue)
      } catch (error) {
        console.warn(`Error setting localStorage key “${key}”:`, error)
      }
    },
    [key, storedValue],
  )

  return [storedValue, setValue]
}

function parseJSON<T>(value: string | null): T | undefined {
  try {
    return value === 'undefined' ? undefined : JSON.parse(value ?? '')
  } catch {
    console.log('parsing error on', { value })
    return undefined
  }
}