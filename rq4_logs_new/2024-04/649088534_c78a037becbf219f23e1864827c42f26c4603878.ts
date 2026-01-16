import { describe, it, expect, jest } from '@jest/globals'
import { wrapWithRollback } from './base'
import type { Result } from './types'


class DummyClient {
  async transaction<T>(callback: (tran: DummyClientTransaction) => Promise<T>): Promise<T> {
    const tran = new DummyClientTransaction()
    return await tran.run(callback)
  }
}

class DummyClientTransaction {
  async run<T>(callback: (client: DummyClientTransaction) => Promise<T>): Promise<T> {
    try {
      const result = await callback(this)
      await this.commit()
      return result
    } catch (e) {
      await this.rollback()
      throw e
    }
  }
  async commit() {}
  async rollback() {}
}


const dummyClientIntendedRollback = async <T>(
  dummyClient: DummyClient,
  rollback: boolean,
  innerBlock: (tran: DummyClientTransaction) => Promise<T>,
): Promise<Result<T>> => {
  const wrapped = wrapWithRollback<DummyClient, DummyClientTransaction, T>(
    async (dummyClient, callback) => await dummyClient.transaction(callback),
  )
  return await wrapped(dummyClient, rollback, innerBlock)
}

describe('dummyClientIntendedRollback', () => {
  it('should return success with rollback', async () => {
    const dummyClient = new DummyClient()
    const result = await dummyClientIntendedRollback(dummyClient, true, async (tran) => {
      return 'result'
    })
    expect(result).toEqual({
      success: true,
      content: 'result',
      rollback: {
        occurred: true,
        intended: true,
      },
    })
  })
  it('should return success without rollback', async () => {
    const dummyClient = new DummyClient()
    const result = await dummyClientIntendedRollback(dummyClient, false, async (tran) => {
      return 'result'
    })
    expect(result).toEqual({
      success: true,
      content: 'result',
      rollback: {
        occurred: false,
        intended: false,
      },
    })
  })
  it('should return failure with rollback', async () => {
    const dummyClient = new DummyClient()
    const result = await dummyClientIntendedRollback(dummyClient, true, async (tran) => {
      throw new Error('error')
    })
    expect(result).toEqual({
      success: false,
      error: new Error('error'),
      rollback: {
        occurred: true,
        intended: false,
      },
    })
  })
  it('should return failure without rollback', async () => {
    const dummyClient = new DummyClient()
    const result = await dummyClientIntendedRollback(dummyClient, false, async (tran) => {
      throw new Error('error')
    })
    expect(result).toEqual({
      success: false,
      error: new Error('error'),
      rollback: {
        occurred: true,
        intended: false,
      },
    })
  })
})