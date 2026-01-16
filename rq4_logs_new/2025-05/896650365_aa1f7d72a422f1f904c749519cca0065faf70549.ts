import { revalidatePath } from 'next/cache'

import { ROUTES } from '@/constants'
import { SUPER_METROID } from '@/test/fixtures'
import {
  gamesServer,
  mockPartialGameRefreshResponse,
  mockRefreshGameFailure,
} from '@/test/servers'

import { refreshGame } from '..'

beforeAll(() => gamesServer.listen())
afterEach(() => {
  vi.clearAllMocks()
  gamesServer.resetHandlers()
})
afterAll(() => gamesServer.close())

describe('refreshGame', () => {
  describe('success', () => {
    it('should revalidate the game preview path', async () => {
      await refreshGame({ game: SUPER_METROID, message: '', status: 'idle' })
      expect(revalidatePath).toHaveBeenCalledWith(
        ROUTES.gamePreview(SUPER_METROID.slug),
      )
    })

    it('should return state', async () => {
      const { response } = mockPartialGameRefreshResponse(SUPER_METROID.slug)
      const state = {
        game: SUPER_METROID,
        message: '',
        status: 'idle',
      } as FirstParameterOf<typeof refreshGame>
      const result = await refreshGame(state)
      expect(result).toEqual({
        ...state,
        responseStatus: response().status,
        status: 'success',
      })
    })
  })

  describe('failure', () => {
    it('should return an error state', async () => {
      const { message, response } = mockRefreshGameFailure(SUPER_METROID.slug)
      const state = {
        game: SUPER_METROID,
        message: '',
        status: 'idle',
      } as FirstParameterOf<typeof refreshGame>
      const result = await refreshGame(state)
      expect(result).toEqual({
        ...state,
        message,
        responseStatus: response().status,
        status: 'failure',
      })
    })

    it('should not revalidate the path', async () => {
      mockRefreshGameFailure(SUPER_METROID.slug)
      const state = {
        game: SUPER_METROID,
        message: '',
        status: 'idle',
      } as FirstParameterOf<typeof refreshGame>
      await refreshGame(state)
      expect(revalidatePath).not.toHaveBeenCalled()
    })
  })
})