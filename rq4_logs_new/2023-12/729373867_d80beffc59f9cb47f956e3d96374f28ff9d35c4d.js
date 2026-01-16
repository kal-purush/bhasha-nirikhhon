import { onBeforeUnmount, onMounted } from 'vue'

export default function detect_outside_click(components, callback) {

  if (!components) return

  if (!Array.isArray(components)) {
    components = [components]
  }


  const listener = (event) => {

    for (const component of components) {
      if (event.target !== component.value && event.composedPath().includes(component.value)) {
        return
      }
    }
    if (typeof callback === 'function') {
      callback()
    }
  }
  onMounted(() => {
    window.addEventListener('click', listener)
  })
  onBeforeUnmount(() => {
    window.removeEventListener('click', listener)
  })

  return { listener }
}