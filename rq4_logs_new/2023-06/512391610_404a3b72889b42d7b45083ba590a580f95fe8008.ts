import FileMD5Worker from './fileMd5.worker?worker'
import { UploadFileChunk } from './UploadFileChunk'
import { MB_BIT, sliceRange } from '~/pages/comp/modules/pool/utils'

export interface UploadFileOptions {
  chunkSize: number // M
}

function calculateChunkList(chunkList: UploadFileChunk[], calculateChunkHash: boolean) {
  const fileMD5Worker = new FileMD5Worker()
  return new Promise<any>((resolve, reject) => {
    const start = new Date().getTime()
    fileMD5Worker.postMessage({ chunkList, calculateChunkHash })
    fileMD5Worker.onmessage = (e) => {
      const end = new Date().getTime()
      fileMD5Worker.terminate()
      resolve({ ...e.data, time: end - start })
    }
    fileMD5Worker.onerror = (e) => {
      reject(e)
    }
  })
}

export class UploadFile {
  name: string
  total: number
  loaded: number
  chunks: UploadFileChunk[]
  options: Partial<UploadFileOptions>
  private _hashReady = false

  constructor(file: File, options?: Partial<UploadFileOptions>) {
    this.total = file.size
    this.name = file.name
    this.loaded = 0

    const chunks: UploadFileChunk[] = []
    if (options?.chunkSize > 0) {
      sliceRange(this.total, options?.chunkSize * MB_BIT, (start, end) => {
        chunks.push(new UploadFileChunk(shortId(), file.slice(start, end, file.type)))
      })
    }
    else {
      chunks.push(new UploadFileChunk(shortId(), file.slice(0, file.size, file.type)))
    }
    this.chunks = chunks
  }

  /**
   * TODO: Queue
   */
  getHash() {
    const start = new Date().getTime()
    return Promise.all([
      calculateChunkList(this.chunks, false),
      calculateChunkList(this.chunks, true),
    ])
      .then(([allHashObj, chunkHashObj]) => {
        const end = new Date().getTime()
        this.chunks.forEach((chunk, index) => {
          chunk.fileHash = allHashObj.hash
          chunk.chunkHash = chunkHashObj.chunkHash?.[index]
        })
        return {
          chunks: this.chunks,
          time: end - start,
        }
      })
  }

  stopHash() {}
}