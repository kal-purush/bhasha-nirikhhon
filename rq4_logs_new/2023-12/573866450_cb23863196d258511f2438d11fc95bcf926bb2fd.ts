export const getSafeArea = () => {
  const el = document.createElement('div')
  el.style.position = 'absolute'
  el.style.visibility = 'hidden'
  el.style.top = 'env(safe-area-inset-top)'
  el.style.right = 'env(safe-area-inset-right)'
  el.style.bottom = 'env(safe-area-inset-bottom)'
  el.style.left = 'env(safe-area-inset-left)'
  document.body.appendChild(el)

  const style = window.getComputedStyle(el)
  document.body.removeChild(el)

  return {
    top: -parseInt(style.top, 10),
    right: -parseInt(style.right, 10),
    bottom: -parseInt(style.bottom, 10),
    left: -parseInt(style.left, 10),
  }
}