import { useEffect } from 'react'

type Props = {
  ref: React.MutableRefObject<any>
  callback(close: boolean): void
}

function useCloseDropdown({ ref, callback }: Props) {
  useEffect(() => {
    function handleSubmenuClose({ type, target, key }: KeyboardEvent) {
      if (type === 'mousedown') {
        if (!ref.current.contains(target)) {
          callback(false)
        }
      }

      if (type === 'keydown') {
        if (key === 'Escape') {
          callback(false)
        }
      }
    }

    const events = ['mousedown', 'keydown']
    events.forEach(type => document.addEventListener(type, handleSubmenuClose))

    return function cleanup() {
      events.forEach(type => document.removeEventListener(type, handleSubmenuClose))
    }
  })
}

export default useCloseDropdown