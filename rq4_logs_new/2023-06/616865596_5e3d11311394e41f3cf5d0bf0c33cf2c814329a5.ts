import { useState, useMemo, useEffect } from 'react'

/**
 * 요소가 뷰포트에 들어가거나 뷰포트에 존재하는지 여부를 반환하는 훅
 * @param ref 추적하려는 요소의 ref
 * @returns 있으면 true, 없다면 false
 */
const useIsInViewport = (ref: any) => {
  const [isIntersecting, setIsIntersecting] = useState(false)

  const observer = useMemo(
    () =>
      new IntersectionObserver(([entry]) => {
        setIsIntersecting(entry.isIntersecting)
      }),
    []
  )

  useEffect(() => {
    observer.observe(ref.current)

    return () => {
      observer.disconnect()
    }
  }, [ref, observer])

  return isIntersecting
}

export default useIsInViewport