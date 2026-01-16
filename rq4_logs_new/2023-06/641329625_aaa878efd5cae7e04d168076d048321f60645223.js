const PENDING = 'pending'
const FULFILLED = 'fulfilled'
const REJECTED = 'rejected'

class newPromise {
  // 原生Promise的状态
  #state = 'pending'
  #result = undefined
  #handlers = []

  constructor(executor) {
    const resolve = (data) => {
      this.#changeState(FULFILLED, data)
    }
    const reject = (err) => {
      this.#changeState(REJECTED, err)
    }
    executor(resolve, reject)
  }
  #isPromiseLike(val) {
    if (val !== null && (typeof val === 'object' || typeof val === 'function')) {
      return typeof val.then === 'function'
    }
    return false
  }

  #runMicrotask(func) {
    setTimeout(func, 0);
    if (typeof process.nextTick === 'object' && typeof process.nextTick === 'function') {
      process.nextTick(func)
    }
  }

  #runOne(callback, resolve, reject) {
    console.log(callback, typeof callback)
    if (typeof callback !== 'function') {
      const settled = this.#state === FULFILLED ? resolve : reject
      console.log(settled, '+++++++++++++')
      settled(this.#result)
      return
    }
    try {
      const data = callback(this.#result)
      if (this.#isPromiseLike(data)) {
        data.then(resolve, reject)
      } else {
        resolve(data)
      }
    } catch (err) {
      reject(err)
    }
  }

  #run() {
    // console.log(this.#handlers)
    if (this.#state === PENDING) return
    while (this.#handlers.length) {
      console.log(this.#handlers.length, this.#state)
      const { onFulfilled, onRejected, reject, resolve } = this.#handlers.shift()
      // console.log(onFulfilled.onRejected,resolve.reject)
      if (this.#state === FULFILLED) {
        // console.log(FULFILLED)
        this.#runOne(onFulfilled, resolve, reject)
      }
      else if (this.#state === REJECTED) {
        this.#runOne(onRejected, resolve, reject)
      }
    }
  }

  // this.#state 改变时调用
  #changeState(state, result) {
    if (this.#state !== PENDING) return
    this.#state = state
    this.#result = result
    this.#run()
  }

  then(onFulfilled, onRejected) {
    return new newPromise((resolve, reject) => {
      this.#handlers.push({
        onFulfilled,
        onRejected,
        resolve,
        reject
      })
      this.#run()
    })
  }


}

// 互动性
// const p = new newPromise((resolve, reject) => {
//   // 异步状态不会影响Promise状态 
//   // resolve(1)
//   setTimeout(() => {
//     // reject(123)
//     resolve(123)
//   }, 1000);
// })
// p.then(res => {
//   console.log('ok1', res)
//   return new Promise((resolve, reject) => {
//     setTimeout(() => {
//       resolve(res * 2)
//     }, 1000)
//   }).then(res => {
//     console.log('ok2', res)
//   })
// })