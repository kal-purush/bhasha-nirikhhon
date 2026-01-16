interface ICSSStyleDeclaration extends CSSStyleDeclaration {
  WebkitOverflowScrolling: string;
}

interface IWindow extends Window {
  preventOverScroll: PreventOverScroll;
}

interface PreventOverScroll {
  enable (): void;

  disable (): void;

  isEnabled (): boolean;
}

;(function (win: Window) {
  let startY: number = 0
  let enabled: boolean = false
  let passiveOption: boolean = false
  try {
    const opts: any = Object.defineProperty({}, 'passive', {
      get () {
        passiveOption = true
      }
    })
    win.addEventListener('test', null, opts)
  } catch (e) {
    // console.log('false')
  }

  function isTouch (evt: any): evt is TouchEvent {
    return !!evt.touches
  }

  function isDocument (evt: any): evt is Document {
    return evt === document
  }

  function handleTouchmove (evt: MouseEvent | TouchEvent): void {
    const zoom: number = win.innerWidth / win.document.documentElement.clientWidth

    if (isTouch(evt) && evt.touches.length > 1 || zoom !== 1) {
      return
    }

    let el: HTMLElement = (evt.target as HTMLElement)
    while (el !== document.body && !isDocument(evt)) {
      const style: CSSStyleDeclaration = win.getComputedStyle(el)

      if (!style) {
        break
      }

      if (el.nodeName === 'INPUT' && el.getAttribute('type') === 'range') {
        return
      }

      const scrolling: string = style.getPropertyValue('-webkit-overflow-scrolling')
      const overflowY: string = style.getPropertyValue('overflow-y')
      const isScrollable: boolean = scrolling === 'touch' && (overflowY === 'auto' || overflowY === 'scroll')
      const canScroll: boolean = el.scrollHeight > el.offsetHeight

      if (isScrollable && canScroll) {
        const height: number = el.clientHeight
        const curY: number = isTouch(evt) ? evt.touches[0].screenY : evt.screenY
        const isAtTop: boolean = (startY <= curY && el.scrollTop === 0)
        const isAtBottom: boolean = (startY >= curY && el.scrollHeight - el.scrollTop === height)
        if (isAtTop || isAtBottom) {
          evt.preventDefault()
        }
        return
      }
      el = (el.parentNode as HTMLElement)
    }
    evt.preventDefault()
  }

  function handleTouchstart (evt: MouseEvent | TouchEvent): void {
    startY = isTouch(evt) ? evt.touches[0].screenY : evt.screenY
  }

  function enable (): void {
    win.addEventListener('touchstart', handleTouchstart, passiveOption ? { passive: false } : false)
    win.addEventListener('touchmove', handleTouchmove, passiveOption ? { passive: false } : false)
    enabled = true
  }

  function disable (): void {
    win.removeEventListener('touchstart', handleTouchstart, false)
    win.removeEventListener('touchmove', handleTouchmove, false)
    enabled = false
  }

  function isEnabled (): boolean {
    return enabled
  }

  function isScrollSupport () {
    const testDiv: HTMLElement = document.createElement('div')
    document.documentElement.appendChild(testDiv)
    ;(testDiv.style as ICSSStyleDeclaration).WebkitOverflowScrolling = 'touch'
    const scrollSupport: boolean = 'getComputedStyle' in win && win.getComputedStyle(testDiv)['-webkit-overflow-scrolling'] === 'touch'
    document.documentElement.removeChild(testDiv)
    if (scrollSupport) {
      enable()
    }
  }

  isScrollSupport()

  ;(win as IWindow).preventOverScroll = {
    enable: enable,
    disable: disable,
    isEnabled: isEnabled
  }
})(window)