import { useEffect, useState } from 'react'

export function useWidth() {
  const [width, setWidth] = useState<number>(10)
  useEffect(() => {
    setWidth(process.stdout.columns)
  }, [])
  return width
}