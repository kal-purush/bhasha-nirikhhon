import { onActivated, onMounted, ref } from 'vue'

export function usePagePosition(elName: string) {
  const scrollTop = ref(0)
  const getEl = () => {
    const el = document.querySelector('.game-app')
    return el
  }
  // onDeactivated(() => {
  //   const el = getEl()
  //   scrollTop.value = el!.scrollTop
  //   console.log(scrollTop.value)
  // })
  onMounted(() => {
    const el = getEl()!
    // console.log(el)
    // el.addEventListener('scroll', (e) => {
    //   scrollTop.value = (e.target as any).scrollTop
    //   console.log(scrollTop.value)
    // })
  })
  onActivated(() => {
    const el = getEl()
    el!.scrollTo({
      top: scrollTop.value,
    })
  })
}