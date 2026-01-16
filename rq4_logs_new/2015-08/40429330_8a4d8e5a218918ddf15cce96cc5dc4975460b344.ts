const blakesBirthday = new Date(1994, 8, 14).getTime()
const keyuesBirthday = new Date(1996, 8, 12).getTime()
const anniversary = new Date(2013, 11, 30).getTime()

const raf = window.requestAnimationFrame || function (fn) { setTimeout(fn, 60) }

const primaryEl = <HTMLElement> document.querySelector('.primary')
const blakesEl = <HTMLElement> document.querySelector('.blake')
const keyuesEl = <HTMLElement> document.querySelector('.keyue')

const SECOND = 1000
const MINUTE = SECOND * 60
const HOUR = MINUTE * 60
const DAY = HOUR * 24
const YEAR = 365.25 * DAY

/**
 * Update each frame.
 */
function update () {
  const now = Date.now()
  const duration = now - anniversary
  const blakesAge = now - blakesBirthday
  const keyuesAge = now - keyuesBirthday

  render(primaryEl, duration, duration * 2 / (blakesAge + keyuesAge))
  render(blakesEl, blakesAge, duration / blakesAge)
  render(keyuesEl, keyuesAge, duration / keyuesAge)

  setTimeout(update, 60)
}

setup(primaryEl)
setup(blakesEl)
setup(keyuesEl)

update()

/**
 * Render updates to the stats.
 */
function render (el: HTMLElement, time: number, percentage: number): void {
  const years = time / YEAR

  el.querySelector('.years').textContent = String(~~years)
  el.querySelector('.decimal').textContent = (years % 1).toFixed(10).toString().substr(1)
  el.querySelector('.percentage').textContent = (percentage * 100).toFixed(6) + '%'
}

/**
 * Setup the element for rendering.
 */
function setup (el: HTMLElement): void {
  el.innerHTML = `
    <div class="timer">
      <span class="years"></span>
      <span class="decimal"></span>
    </div>
    <div class="percentage"></div>
  `
}