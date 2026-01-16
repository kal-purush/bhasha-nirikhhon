import { AllowedUriFacade } from '..'

describe('AllowedUriFacade', () => {
  describe('allowed', () => {
    it('should return false when context.defaultValidate returns false', () => {
      const { allowed } = new AllowedUriFacade('https://ned-not.com', {
        defaultProtocol: 'https',
        defaultValidate: vi.fn().mockReturnValue(false),
        protocols: [],
      })
      expect(allowed).toBe(false)
    })

    it('should return false when the protocol is not allowed', () => {
      const { allowed } = new AllowedUriFacade('ftp://ned-not.com', {
        defaultProtocol: 'https',
        defaultValidate: vi.fn().mockReturnValue(true),
        protocols: [],
      })
      expect(allowed).toBe(false)
    })

    it('should return false when the protocol is not allowed via context', () => {
      const { allowed } = new AllowedUriFacade('https://ned-not.com', {
        defaultProtocol: 'https',
        defaultValidate: vi.fn().mockReturnValue(true),
        protocols: [],
      })
      expect(allowed).toBe(false)
    })

    it('should return true when the protocol is allowed', () => {
      const { allowed } = new AllowedUriFacade('https://ned-not.com', {
        defaultProtocol: 'https',
        defaultValidate: vi.fn().mockReturnValue(true),
        protocols: ['https', { scheme: '' }],
      })
      expect(allowed).toBe(true)
    })

    it('should return false when an error is thrown', () => {
      const { allowed } = new AllowedUriFacade('https://ned-not.com', {
        defaultProtocol: 'https',
        defaultValidate: vi.fn().mockImplementation(() => {
          throw new Error('message')
        }),
        protocols: ['https'],
      })
      expect(allowed).toBe(false)
    })

    it('should handle a partial url', () => {
      const { allowed } = new AllowedUriFacade('ned-not.com', {
        defaultProtocol: 'https',
        defaultValidate: vi.fn().mockReturnValue(true),
        protocols: ['https'],
      })
      expect(allowed).toBe(true)
    })
  })
})