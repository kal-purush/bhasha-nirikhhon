import { type RefObject } from 'preact'
import { useEffect } from 'preact/hooks'

export function useOnClickOutside(ref: RefObject<HTMLElement>, handler: (event: MouseEvent | TouchEvent) => void) {
  useEffect(() => {
    function handleEvent(event: MouseEvent | TouchEvent) {
      if (!ref.current || (event.target instanceof HTMLElement && ref.current.contains(event.target))) {
        return
      }

      handler(event)
    }

    document.addEventListener('mousedown', handleEvent)
    document.addEventListener('touchstart', handleEvent)

    return () => {
      document.removeEventListener('mousedown', handleEvent)
      document.removeEventListener('touchstart', handleEvent)
    }
  }, [handler, ref])
}