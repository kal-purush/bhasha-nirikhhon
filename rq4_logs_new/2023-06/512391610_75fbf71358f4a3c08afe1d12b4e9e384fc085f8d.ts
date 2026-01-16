// import WorkerUri from '../worker/fileMd5.worker?worker'
import workerpool from 'workerpool'

class Task {
  private _ready = false
  constructor(public name: string) {
  }

  start() {
    this._ready = false
  }

  terminal() {
    this._ready = true
  }
}

function createPool() {
  const queue: any[] = []

  // const workMap = new WeakMap<any, any>()

  for (let i = 0; i < 6; i++) {
    const task = new Task(`task_${i}`)
    queue.push(task)
  }

  // workMap.set('1', () => {})
  // queue.push(worker1)
  // queue.push(worker2)

  function _processNext() {

  }

  console.log({ queue })
  return {
    queue,
    exec(task: Task, args: any[]) {
      return new Promise((resolve, reject) => {
        queue.push({
          task,
          args,
          resolve,
          reject,
        })
        _processNext()
      })
    },
  }
}

export function runTest() {
  const { queue, exec } = createPool()
}

let index = 0
const pool = workerpool.pool({
  // maxWorkers: 6,
  minWorkers: 2,
})
workerpool.pool({})
export function runWorkerPool() {
  async function add(a: number, b: number) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(a + b)
      }, Math.random() * 5000)
    })
  }

  pool
    .exec(add, [3, 4])
    .then((result) => {
      console.log('result', result, workerpool) // outputs 7
    })
    .catch((err) => {
      console.error(err)
    })
    .then(() => {
      setTimeout(() => {
        if (index === 0) {
          console.log('termiaml')

          pool.terminate()
          index++
        }
      }, 5000)
    })
}