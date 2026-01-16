import _layout from './layout'

describe('layout module', () => {
  it('should create a layouts array on the state object if one does not exist', () => {
    const state = {}
    const { layout, setLayout } = _layout(state)

    expect(state.hasOwnProperty('layouts')).toBe(true)
    expect(Array.isArray(state.layouts)).toBe(true)
  })
})

describe('api.layout', () => {
  let state;
  let api;

  beforeEach(() => {
    state = {
      id: '1',
      data: {},
      layouts: []
    }

    api = _layout(state)
  })

  it('should be a function', () => {
    expect(typeof api.layout).toBe('function')
  })

  it('should add a layout to the state.layouts array when called', () => {
    api.layout({
      name: 'main',
      size: '2x2',
      default: true,
      render() {
        return 'noice'
      }
    })

    expect(state.layouts[0]).toEqual(
      expect.objectContaining({
        name: expect.any(String),
        size: expect.any(String),
        default: expect.any(Boolean),
        render: expect.any(Function)
      })
    )
  })
})

describe('api.setLayout', () => {
  it('should be a function', () => {
    const state = { id: '1', data: {}, layouts: {} }
    const { setLayout } = _layout(state)

    expect(typeof setLayout).toBe('function')
  })
})