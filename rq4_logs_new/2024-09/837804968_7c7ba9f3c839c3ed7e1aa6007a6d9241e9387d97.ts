import type { Nullable } from '~/types'

export interface UseSelectionOptions {
  /**
   * Target selector
   */
  target?: string

  /**
   * @default `__ELEMENT_SELECTION_OVERLAY__`
   */
  overlayClass?: string

  document?: Document

  onSelect?: (el: HTMLElement) => void

  onClear?: () => void
}

export interface Position {
  x: number
  y: number
}

export const EL_OVERLAY_CLASS = '__ELEMENT_SELECTION_OVERLAY__'

export function useElementSelection(
  elRef: Ref<Nullable<HTMLElement>>,
  options: UseSelectionOptions = {},
) {
  const doc = options.document || window.document

  const [isSelecting, setIsSelecting] = useToggle()
  const { width: windowWidth, height: windowHeight } = useWindowSize()

  const overlayRef = shallowRef<Nullable<HTMLElement>>(null)
  const { top, left, right, bottom } = useElementBounding(overlayRef)

  const startPosition = ref<Nullable<Position>>(null)
  const stopPosition = ref<Nullable<Position>>(null)

  function getTargets() {
    if (!elRef.value || !options.target) return []
    return elRef.value.querySelectorAll<HTMLElement>(options.target)
  }
  function ensureOverlay() {
    overlayRef.value = doc.querySelector<HTMLElement>(`.${EL_OVERLAY_CLASS}`)
    if (!overlayRef.value) {
      const overlayEl = doc.createElement('div')
      overlayEl.classList.add(EL_OVERLAY_CLASS)
      overlayEl.style.position = 'fixed'
      overlayEl.style.top = '0'
      overlayEl.style.left = '0'
      overlayEl.style.zIndex = '99999'
      overlayEl.style.pointerEvents = 'none'
      overlayEl.style.border = '1px solid red'
      overlayEl.style.backgroundColor = 'rgba(255, 0, 0, 0.1)'
      doc.body.append(overlayEl)
      overlayRef.value = overlayEl
    }
    return overlayRef.value
  }

  function checkIntersection() {
    const targets = getTargets()

    targets.forEach(target => {
      const targetRect = target.getBoundingClientRect()

      if (
        targetRect.top > top.value &&
        targetRect.bottom < bottom.value &&
        targetRect.left > left.value &&
        targetRect.right < right.value
      ) {
        options.onSelect?.(target)
      }
    })
  }

  function updateRect() {
    if (!startPosition.value || !stopPosition.value || !isSelecting.value) return

    const overlay = ensureOverlay()

    if (stopPosition.value.x > startPosition.value.x) {
      overlay.style.left = `${startPosition.value.x}px`
      overlay.style.right = 'auto'
    } else {
      overlay.style.right = `${windowWidth.value - startPosition.value.x}px`
      overlay.style.left = 'auto'
    }

    if (stopPosition.value.y > startPosition.value.y) {
      overlay.style.top = `${startPosition.value.y}px`
      overlay.style.bottom = 'auto'
    } else {
      overlay.style.bottom = `${windowHeight.value - startPosition.value.y}px`
      overlay.style.top = 'auto'
    }

    const width = Math.abs(stopPosition.value.x - startPosition.value.x)
    const height = Math.abs(stopPosition.value.y - startPosition.value.y)

    overlay.style.width = `${width}px`
    overlay.style.height = `${height}px`

    checkIntersection()
  }

  addEventListener('mousedown', (evt: MouseEvent) => {
    setIsSelecting(true)
    options.onClear?.()

    startPosition.value = { x: evt.clientX, y: evt.clientY }
    stopPosition.value = null
  })
  addEventListener('mousemove', (evt: MouseEvent) => {
    if (!isSelecting.value) return

    stopPosition.value = { x: evt.clientX, y: evt.clientY }
    updateRect()
  })
  addEventListener('mouseup', (evt: MouseEvent) => {
    if (!isSelecting.value) return

    stopPosition.value = { x: evt.clientX, y: evt.clientY }
    updateRect()
    setIsSelecting(false)
  })

  tryOnBeforeUnmount(() => {
    // Clear dom
    if (overlayRef.value) {
      overlayRef.value.remove()
    }
  })

  return {
    isSelecting,

    getTargets,
    updateRect,
  }
}