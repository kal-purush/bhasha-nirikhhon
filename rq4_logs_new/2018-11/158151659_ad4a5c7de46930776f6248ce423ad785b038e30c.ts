import * as reducers from '.'

describe('Todos reducers test', () => {
  test('should return initial state', () => {
    expect(reducers.todos(undefined, {} as any)).toEqual([])
  })

  test('should handle ADD_TODO ', () => {
    const text = "Rock'n ROll"
    expect(reducers.todos([], reducers.addTodo(text))).toMatchObject([
      {
        complete: false,
        text
      }
    ])
  })

  test('should handle TOGGLE_TODO ', () => {
    expect(
      reducers.todos(
        [
          {
            complete: false,
            id: '0',
            text: 'テスト'
          },
          {
            complete: false,
            id: '1',
            text: 'テスト'
          }
        ],
        reducers.toggleTodo(0)
      )[0].complete
    ).toBe(true)
  })
})