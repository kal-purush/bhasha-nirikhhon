
/**
 * 进制转换
 * @ignore
 * @param {string} value 转换的值
 * @param {number} from 将 from进制(2|8|10|16) 转成 to 进制
 * @param {number} to 目标进制
 */
const _fn = (value: string, from: number, to: number): string => {
  return parseInt(value, from).toString(to);
}

const _toBinary = (value: string, from: number): string => {
  return _fn(value, from, 2)
}

const _toOctal = (value: string, from: number): string => {
  return _fn(value, from, 8)
}

const _toDecimal = (value: string, from: number): string => {
  const radix =  _fn(value, from, 10)
  return +radix < 10 ? `0${radix}`: radix
}

const _toHex = (value: string, from: number): string => {
  const radix = _fn(value, from, 16)
  return +radix < 16 ? `0${radix}`: radix
}

const _fillLength = (value: string|number, len: number): string => {
  return (new Array(len + 1).fill('0').join('') + value).slice(-len)
}

export class Converter {
  value:string = ''
  from: number = 10
  constructor(value: string, from: number) {
    if (!value || !from) return this
    this.value = value
    this.from = from
    return this
  }
  toBinary () {
   return  _toBinary(this.value, this.from)
  }
  toOctal () {
    return  _toOctal(this.value, this.from)
  }
  toDecimal () {
    return  _toDecimal(this.value, this.from)
  }
  toHex () {
    return  _toHex(this.value, this.from)
  }
  fill (len) {
    return _fillLength(this.value, len)
  }
}

export const toBinary = _toBinary
export const toOctal = _toOctal
export const toDecimal = _toDecimal
export const toHex = _toHex
export const fillLength = _fillLength