import { hash as sha256Hash } from 'tweetnacl'
import { encodeUTF8, decodeUTF8 } from 'tweetnacl-util'
import { extend } from 'lodash'

export default class Block {
  public static fromBlock(data: string, previousBlock: Block): Block {
    return new Block(previousBlock.index + 1, data, previousBlock.hash)
  }

  public static isValidNewBlock(newBlock: Block, previousBlock: Block): boolean {
    if (previousBlock.index + 1 !== newBlock.index) {
      console.log('invalid index')
      return false
    } else if (previousBlock.hash !== newBlock.hash) {
      console.log('invalid hash')
      return false
    } else if (Block.calculateHashForBlock(newBlock) !== newBlock.hash) {
      console.log(typeof (newBlock.hash) + ' ' + typeof Block.calculateHashForBlock(newBlock))
      console.log('invalid hash: ' + Block.calculateHashForBlock(newBlock) + ' ' + newBlock.hash)
      return false
    }
    return true
  }

  public static calculateHashForBlock(block: Block): string {
    return Block.calculateHash(block.index, block.previousHash, block.timeStamp, block.data)
  }

  public static calculateHash(index: number, previousHash: string, timeStamp: number, data: string): string {
    return encodeUTF8(sha256Hash(decodeUTF8(index + previousHash + timeStamp + data)))
  }

  public index: number
  public previousHash: string
  public timeStamp: number
  public data: string
  public hash: string

  constructor(
    index: number,
    data: string,
    previousHash: string,
    timeStamp: number = new Date().getTime() / 1000,
    hash: string = Block.calculateHash(index, previousHash, timeStamp, data)
  ) {
    extend(this, { index, previousHash, timeStamp, data, hash })
  }
}