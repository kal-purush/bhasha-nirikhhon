/**
 * 生成指定区间的随机数
 * @param {number} min 区间的最小数
 * @param {number} max 区间的最大数
 * @returns {number}
 */
export function random (min: number, max?: number): number {
  if (max === undefined) {
    max = min
    min = 0
  }

  return Math.random() * max + min
}

/**
 * 用Promise包装的延迟等待函数
 * @param {number} milliseconds 延迟时间，毫秒为单位
 * @returns {Promise<void>}
 */
export function wait (milliseconds: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}