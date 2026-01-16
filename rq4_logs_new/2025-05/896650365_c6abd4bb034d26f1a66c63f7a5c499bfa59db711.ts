import { PublishedParamFacade } from '../publishedParamFacade'

describe('PublishedParamFacade', () => {
  describe('setOrDeletePublishedParam', () => {
    it('should do nothing when published is false and the param is unset', () => {
      const params = {
        published: false,
        searchParams: new URLSearchParams(),
      }
      new PublishedParamFacade(params).setOrDeletePublishedParam()
      expect(params.searchParams.get('published')).toBeNull()
    })

    it('should set the param when published is true', () => {
      const params = {
        published: true,
        searchParams: new URLSearchParams(),
      }
      new PublishedParamFacade(params).setOrDeletePublishedParam()
      expect(params.searchParams.get('published')).toBe('true')
    })

    it('should not set the param when published is true', () => {
      const params = {
        published: true,
        searchParams: {
          has: vi.fn().mockReturnValue('true'),
          set: vi.fn(),
        } as unknown as URLSearchParams,
      }
      new PublishedParamFacade(params).setOrDeletePublishedParam()
      expect(params.searchParams.set).not.toHaveBeenCalled()
    })

    it('should not delete the param when it is not present', () => {
      const params = {
        published: false,
        searchParams: {
          delete: vi.fn(),
          has: vi.fn().mockReturnValue(null),
        } as unknown as URLSearchParams,
      }
      new PublishedParamFacade(params).setOrDeletePublishedParam()
      expect(params.searchParams.delete).not.toHaveBeenCalled()
    })
  })
})