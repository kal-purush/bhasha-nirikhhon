import { sleep } from '~/utils/utils'

function cancelAbleWrap<T = any>(func: () => Promise<T>) {
  let canceled = false

  function start() {
    return new Promise((resolve, reject) => {
      // 如果太耗时的任务取消，会不会内存泄露？
      func().then((res) => {
        if (canceled) {
          console.log('cancel')
          return
        }

        resolve(res)
      }).catch((err) => {
        if (canceled) {
          console.log('cancel')
          return
        }

        reject(err)
      })
    })
  }

  function restart() {
    canceled = false
    return start()
  }

  function cancel() {
    canceled = true
  }

  return {
    start,
    restart,
    cancel,
  }
}

const p = cancelAbleWrap(() => sleep(3000))

p.start().then((res) => {
  console.log('ppp', res)
})
setTimeout(() => {
  p.cancel()
}, 1200)