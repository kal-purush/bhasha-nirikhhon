import { SUPER_METROID } from '@/test/fixtures'
import { gamesServer, mockGameRequestFailure } from '@/test/servers'

import { generateGameMetadata } from '..'

beforeAll(() => gamesServer.listen())
afterEach(() => gamesServer.resetHandlers())
afterAll(() => gamesServer.close())

describe('generateGameMetadata', () => {
  it('should return "Not Found" data when the game is not found', async () => {
    mockGameRequestFailure()
    const result = await generateGameMetadata({
      pageParams: Promise.resolve({ slug: SUPER_METROID.slug }),
      type: 'show',
    })
    expect(result).toEqual({ title: 'Not Found' })
  })

  it('should return admin show type metadata', async () => {
    const result = await generateGameMetadata({
      pageParams: Promise.resolve({ slug: SUPER_METROID.slug }),
      type: 'admin-show',
    })
    expect(result).toEqual({
      description: SUPER_METROID.summary,
      title: `Admin: ${SUPER_METROID.name}`,
    })
  })

  it('should return preview show type metadata', async () => {
    const result = await generateGameMetadata({
      pageParams: Promise.resolve({ slug: SUPER_METROID.slug }),
      type: 'preview-show',
    })
    expect(result).toEqual({
      description: SUPER_METROID.summary,
      title: `Preview: ${SUPER_METROID.name}`,
    })
  })

  it('should return show type metadata', async () => {
    const result = await generateGameMetadata({
      pageParams: Promise.resolve({ slug: SUPER_METROID.slug }),
      type: 'show',
    })
    expect(result).toEqual({
      description: SUPER_METROID.summary,
      title: SUPER_METROID.name,
    })
  })

  it('should return edit type metadata', async () => {
    const result = await generateGameMetadata({
      pageParams: Promise.resolve({ slug: SUPER_METROID.slug }),
      type: 'edit',
    })
    expect(result).toEqual({
      description: SUPER_METROID.summary,
      title: `Edit: ${SUPER_METROID.name}`,
    })
  })
})