import * as actions from '.'

describe('Todos actions test', () => {
  test('should create an action to add a todo', () => {
    const text = "Rock'n ROLL"
    const expectAction = {
      text,
      type: actions.ADD_TODO
    }
    expect(actions.addTodo(text)).toEqual(expectAction)
  })

  test('should create an action to toggle a todo', () => {
    const index = 2
    const expectAction = {
      index,
      type: actions.TOGGLE_TODO
    }
    expect(actions.toggleTodo(index)).toEqual(expectAction)
  })
})