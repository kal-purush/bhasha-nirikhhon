import { Dispatch, SetStateAction, useEffect, useState } from 'react'

const getItem = (key: string) => {
  const value = sessionStorage.getItem(key)
  return value ? JSON.parse(value) : undefined
}

const setItem = <S>(key: string, value: S) => {
  sessionStorage.setItem(key, JSON.stringify(value))
}

export const useSession = <S>(key: string, initialValue: S): [S, Dispatch<SetStateAction<S>>] => {
  const [value, setValue] = useState<S>(getItem(key) || initialValue)
  useEffect(() => setItem(key, value), [key, value])
  return [value, setValue]
}