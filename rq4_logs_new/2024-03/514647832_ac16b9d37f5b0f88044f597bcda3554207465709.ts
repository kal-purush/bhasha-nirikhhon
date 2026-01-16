import { describe, expect, test, vi } from 'vitest'
import * as narouApi from '~/datasource/api/narou'
import * as narouRepository from '~/datasource/d1/narou'
import { addNarouNovel, listNarouNovel, removeNarouNovel, updateNarouNovel } from '~/usecase/narou'

// @ts-ignore
const d1: D1Database = 'd1_stub'

describe('listNarouNovel', () => {
  test('データがない場合、空配列が返されること', async () => {
    const spyRepositoryListAll = vi.spyOn(narouRepository, 'listAll').mockResolvedValue([])

    await expect(listNarouNovel(d1, 'userAgent')).resolves.toStrictEqual([])
    expect(spyRepositoryListAll).toHaveBeenCalledTimes(1)
    expect(spyRepositoryListAll).toHaveBeenCalledWith(d1)
  })

  test('データがある場合、一覧の配列が取得できること', async () => {
    const spyRepositoryListAll = vi.spyOn(narouRepository, 'listAll').mockResolvedValue([
      { ncode: 'n0000zz', currentPage: 1 },
      { ncode: 'n1111zz', currentPage: 0 },
    ])
    const spyFetchNarouApi = vi
      .spyOn(narouApi, 'fetchNarouApi')
      // @ts-ignore
      .mockResolvedValue([
        { allcount: 2 },
        {
          ncode: 'n0000zz',
          title: 'title0000',
          general_all_no: 10,
          general_lastup: '2024-01-01 00:00:00',
        },
        {
          ncode: 'n1111zz',
          title: 'title1111',
          general_all_no: 10,
          general_lastup: '2024-01-01 00:00:00',
        },
      ])

    await expect(listNarouNovel(d1, 'userAgent')).resolves.toStrictEqual([
      {
        ncode: 'n0000zz',
        title: 'title0000',
        currentPage: 1,
        totalPage: 10,
        lastPublishedAt: '2024年1月1日0時0分',
      },
      {
        ncode: 'n1111zz',
        title: 'title1111',
        currentPage: 0,
        totalPage: 10,
        lastPublishedAt: '2024年1月1日0時0分',
      },
    ])
    expect(spyRepositoryListAll).toHaveBeenCalledTimes(1)
    expect(spyRepositoryListAll).toHaveBeenCalledWith(d1)
    expect(spyFetchNarouApi).toHaveBeenCalledTimes(1)
    expect(spyFetchNarouApi).toHaveBeenCalledWith('n0000zz-n1111zz', 'userAgent')
  })
})

describe('addNarouNovel', () => {
  test('新規に追加する小説ではなくページが同じ場合、処理をスキップする', async () => {
    const spyRepositoryGetOne = vi
      .spyOn(narouRepository, 'getOne')
      .mockResolvedValue({ ncode: 'n0000zz', currentPage: 1 })

    await expect(addNarouNovel(d1, 'https://ncode.syosetu.com/n0000zz/1')).resolves.toBe(
      'n0000zz/1は重複のためスキップしました',
    )
    expect(spyRepositoryGetOne).toHaveBeenCalledTimes(1)
    expect(spyRepositoryGetOne).toHaveBeenCalledWith(d1, { ncode: 'n0000zz' })
  })

  test('新規に追加する小説ではなくページが異なる場合、ページを更新する', async () => {
    const spyRepositoryGetOne = vi
      .spyOn(narouRepository, 'getOne')
      .mockResolvedValue({ ncode: 'n0000zz', currentPage: 1 })
    const spyRepositoryUpdate = vi.spyOn(narouRepository, 'update').mockResolvedValue()

    await expect(addNarouNovel(d1, 'https://ncode.syosetu.com/n0000zz/2')).resolves.toBe(
      'n0000zzはページを2で更新しました',
    )
    expect(spyRepositoryGetOne).toHaveBeenCalledTimes(1)
    expect(spyRepositoryGetOne).toHaveBeenCalledWith(d1, { ncode: 'n0000zz' })
    expect(spyRepositoryUpdate).toHaveBeenCalledTimes(1)
    expect(spyRepositoryUpdate).toHaveBeenCalledWith(d1, { ncode: 'n0000zz', currentPage: 2 })
  })

  test('新規に追加する小説の場合', async () => {
    const spyRepositoryGetOne = vi.spyOn(narouRepository, 'getOne').mockResolvedValue(undefined)
    const spyRepositoryInsert = vi.spyOn(narouRepository, 'insert').mockResolvedValue()

    await expect(addNarouNovel(d1, 'https://ncode.syosetu.com/n0000zz/1')).resolves.toBe(
      'n0000zz/1が新しく追加されました',
    )
    expect(spyRepositoryGetOne).toHaveBeenCalledTimes(1)
    expect(spyRepositoryGetOne).toHaveBeenCalledWith(d1, { ncode: 'n0000zz' })
    expect(spyRepositoryInsert).toHaveBeenCalledTimes(1)
    expect(spyRepositoryInsert).toHaveBeenCalledWith(d1, { ncode: 'n0000zz', currentPage: 1 })
  })
})

describe('removeNarouNovel', () => {
  test('なろう小説が削除されること', async () => {
    const spyRepositoryRemove = vi.spyOn(narouRepository, 'remove').mockResolvedValue()

    await expect(removeNarouNovel(d1, 'n0000zz')).resolves.toBeUndefined()
    expect(spyRepositoryRemove).toHaveBeenCalledTimes(1)
    expect(spyRepositoryRemove).toHaveBeenCalledWith(d1, { ncode: 'n0000zz' })
  })
})

describe('updateNarouNovel', () => {
  test('なろう小説が更新されること', async () => {
    const spyRepositoryUpdate = vi.spyOn(narouRepository, 'update').mockResolvedValue()

    await expect(updateNarouNovel(d1, 'n0000zz', 0)).resolves.toBeUndefined()
    expect(spyRepositoryUpdate).toHaveBeenCalledTimes(1)
    expect(spyRepositoryUpdate).toHaveBeenCalledWith(d1, { ncode: 'n0000zz', currentPage: 0 })
  })
})