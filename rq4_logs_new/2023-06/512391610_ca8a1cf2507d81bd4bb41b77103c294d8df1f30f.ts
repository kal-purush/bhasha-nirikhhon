interface HashingRequest {
  blob: Blob
  resolve: (...d: any) => void
  reject: (...d: any) => void
}
export interface WorkerOptions {
  credentials?: 'omit' | 'same-origin' | 'include'
  name?: string
  type?: 'classic' | 'module'
}
export class FileParallelHasher {
  private _queue: HashingRequest[] // 运行队列
  private _hashWorker: Worker
  private _processing?: HashingRequest // 当前运行的

  private _ready = true

  constructor(workerUri: string, workerOptions: WorkerOptions) {
    if (Worker) {
      this._hashWorker = new Worker(workerUri, workerOptions)
      this._hashWorker.onmessage = this._receivedMessage.bind(this)
      this._hashWorker.onerror = (err: any) => {
        this._ready = false
        console.error('Hash worker failure', err)
      }
    }
    else {
      this._ready = false
      console.error('Web Workers are not supported in this browser')
    }
  }

  public hash(blob: Blob) {
    return new Promise((resolve, reject) => {
      this._queue.push({
        blob,
        resolve,
        reject,
      })
      this._processNext()
    })
  }

  private _receivedMessage(evt: any) {
    const data = evt.data
    if (data.success)
      this._processing.resolve(data.data)
    else
      this._processing.reject(data.data)

    this._processing = undefined
  }

  private _processNext() {
    if (this._ready && !this._processing && this._queue.length) {
      this._processing = this._queue.pop()
      this._hashWorker.postMessage(this._processing.blob)
    }
  }

  public terminate() {
    this._ready = false
    this._hashWorker.terminate()
  }
}