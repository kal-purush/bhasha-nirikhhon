export type TaskStatus = 'IDLE' | 'SUCCESS' | 'ERROR' | 'PENDING' | 'STOP'

export const MB_BIT = 1024 * 1024

/**
 * 生成range
 *
 * @param end
 * @param offsetSize
 * @param cb
 */
export function sliceRange(end: number, offsetSize: number, cb?: (start: number, end: number) => void) {
  let offset = 0
  const range: number[][] = []
  while (offset < end) {
    const _r: [number, number] = [offset, 0]
    offset += offsetSize
    _r[1] = offset > end ? end : offset
    cb && cb(_r[0], _r[1])
    range.push(_r)
  }
  // console.log('sliceResult', {
  //   offset,
  //   range,
  //   end,
  // })
  return range
}

export function getFilenameExt(name: string) {
  return name.substring(name.lastIndexOf('.') + 1)
}

/**
 * @description 获取文件二进制数据
 * @param {File} file 文件对象实例
 * @param {Object} options 配置项，指定读取的起止范围
 */
function getArrayBuffer(file: File, { start = 0, end = 32 }) {
  return new Promise((resolve, reject) => {
    try {
      // 定义FileReader实例
      const reader = new FileReader()
      reader.onload = (e) => {
        // 获取文件二进制数据
        const buffers = new Uint8Array(e.target.result as ArrayBuffer)
        resolve(buffers)
        // 以一个png图片为例，其数据如下：
        // Uint8Array(32) [137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 64, 0, 0, 0, 64, 8, 6, 0, 0, 0, 170, 105, 113]
      }
      reader.onerror = err => reject(err)
      reader.onabort = err => reject(err)
      // 读取文件
      reader.readAsArrayBuffer(file.slice(start, end))
    }
    catch (err) {
      reject(err)
    }
  })
}

const signatureList = [
  {
    mime: 'video/mp4', // MIME Type
    ext: 'mp4', // Extension(s)
    offset: 4, // Offset
    signature: [0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6F, 0x6D], // Hex Signature
  },
  // ... more
]

/**
 * @description 校验给出的字节数据是否符合某种MIME Type的signature
 * @param {Array} buffers 字节数据
 * @param signature
 * @param offset
 */
function checkBuffer(buffers: Uint8Array, signature: number[], offset = 0) {
  for (let i = 0, len = signature.length; i < len; i++) {
    // 传入字节数据与文件signature不匹配
    // 需考虑有offset的情况以及signature中有值为undefined的情况
    if (buffers[i + offset] !== signature[i] && signature[i] !== undefined)
      return false
  }
  return true
}