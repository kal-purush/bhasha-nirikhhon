import HashWorker from './hash.worker.ts?worker'

const BLOCK_SIZE = 10 * 1024 * 1024 // 10M
type ChunkStatus = 'READY' | 'UPLOADING' | 'SUCCESS' | 'ERROR'

export class FileChunk {
  progress = { loaded: 0 }
  readonly fileName: string
  chunkName = ''
  // 切片 Hash 值
  fileHash = ''
  // 索引
  index = 0
  // 文件切片数
  chunkNum = 0
  // 文件状态 'READY', 'UPLOADING', 'SUCCESS', 'ERROR'
  status: ChunkStatus = 'READY'
  readonly start: number
  readonly end: number
  readonly total: number
  readonly chunk: Blob
  constructor(chunk: Blob, fileName: string, start: number, end: number, total: number) {
    this.chunk = chunk
    this.fileName = fileName
    // 切片起始位置
    this.start = start
    // 切片结束位置
    this.end = end
    // 文件总大小
    this.total = total
  }

  toFormData() {
    const formData = new FormData()
    formData.append('chunk', this.chunk)
    formData.append('fileHash', this.fileHash)
    formData.append('chunkName', this.chunkName)
    formData.append('chunkNum', `${this.chunkNum}`)
    formData.append('start', `${this.start}`)
    formData.append('end', `${this.end}`)
    formData.append('total', `${this.total}`)
    formData.append('index', `${this.index}`)
    formData.append('fileName', this.fileName)

    return formData
  }
}

/**
 * 生成文件切片
 * @param file
 * @param blockSize 切片大小
 */
export function createFileChunk(file: File, blockSize = BLOCK_SIZE) {
  const fileChunkList: FileChunk[] = []
  const { name, size } = file
  let cur = 0
  while (cur < size) {
    let end = cur + blockSize
    if (end > size)
      end = size

    fileChunkList.push(new FileChunk(file.slice(cur, end), name, cur, end, size))
    cur += blockSize
  }
  const chunkNum = fileChunkList.length
  fileChunkList.forEach((chunkFile, index) => {
    chunkFile.index = index
    chunkFile.chunkName = `${chunkFile.fileName}_${index}`
    chunkFile.chunkNum = chunkNum
  })

  return fileChunkList
}

let hashWorker: Worker

export function calculateFileHash(fileChunkList: FileChunk[]) {
  return new Promise<{ hash: string; time: number }>((resolve, reject) => {
    !hashWorker && (hashWorker = new HashWorker())
    const start = new Date().getTime()
    hashWorker.postMessage({ fileChunkList, type: 'HASH' })
    hashWorker.onmessage = (e) => {
      const end = new Date().getTime()
      // console.log(`hashWorker  ${end - start}ms`, e.data)
      resolve({ ...e.data, time: end - start })
    }
    hashWorker.onerror = (e) => {
      reject(e)
    }
  })
}