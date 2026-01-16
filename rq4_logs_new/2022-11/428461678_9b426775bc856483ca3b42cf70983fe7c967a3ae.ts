
export const RouterMock = {
    route: '/',
    pathname: '',
    query: '',
    asPath: '',
    push: jest.fn(() => {
        console.log('pushing mock')
    }),
    events: {
        on: jest.fn(),
        off: jest.fn()
    },
    beforePopState: jest.fn(() => null),
    prefetch: jest.fn(() => null)
};