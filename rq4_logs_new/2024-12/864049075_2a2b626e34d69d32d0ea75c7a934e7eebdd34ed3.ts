import { useState, useEffect, useRef } from 'react'

export function useFitText(minFontSize = 1, maxFontSize = 320, step = 0.6) {
  const [fontSize, setFontSize] = useState(maxFontSize)
  const textRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const resizeObserver = new ResizeObserver(() => {
      adjustFontSize()
    })

    if (textRef.current) {
      resizeObserver.observe(textRef.current)
    }

    return () => resizeObserver.disconnect()
  }, [])

  const adjustFontSize = () => {
    if (!textRef.current) return

    let low = minFontSize
    let high = maxFontSize

    while (low <= high) {
      const mid = low + (high - low) / 2
      const roundedMid = Math.round(mid * 2) / 2 // Round to nearest 0.5

      textRef.current.style.fontSize = `${roundedMid}px`

      const { scrollWidth, clientWidth, scrollHeight, clientHeight } = textRef.current

      if (scrollWidth <= clientWidth && scrollHeight <= clientHeight) {
        low = roundedMid + step
      } else {
        high = roundedMid - step
      }
    }

    setFontSize(Math.floor(high))
  }

  return { fontSize, textRef }
}