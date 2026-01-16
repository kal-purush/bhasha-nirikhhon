import React from 'react'

export const useOutsideClick = <T>(callback: () => void) => {
  const ref = React.useRef<T>(null)

  React.useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (!(event.target instanceof HTMLElement)) return
      if (!(ref.current instanceof HTMLElement)) return
      if (ref.current.contains(event.target)) return
      callback()
    }
    document.addEventListener('click', handleClickOutside, true)
    return () => document.removeEventListener('click', handleClickOutside, true)
  }, [])

  return ref
}