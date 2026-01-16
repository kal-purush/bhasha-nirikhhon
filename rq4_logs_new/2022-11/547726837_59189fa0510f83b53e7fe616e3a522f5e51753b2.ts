import { ReactElement, RefObject, useCallback, useRef } from "react"

export const useScroll = (): [RefObject<HTMLDivElement>, () => void] => {
  const ref = useRef<HTMLDivElement>(null)
  const moveTo = useCallback(() => {
    ref?.current?.scrollTo(0, 0)
  }, [])
  return [ref, moveTo]
}