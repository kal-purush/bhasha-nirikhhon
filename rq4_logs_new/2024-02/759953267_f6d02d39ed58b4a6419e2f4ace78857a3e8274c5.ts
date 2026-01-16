
const MY_LIST_TODOS = '@MyListTodos'

export type Todo = {
  todo: string
  done: boolean
}

export const useStorage = () => {

  const setItem = (todo: Todo) =>  {

    const dataSaved = getItems()

      if(dataSaved) {
        dataSaved.unshift(todo)
        localStorage.setItem(MY_LIST_TODOS, JSON.stringify(dataSaved));
      } else {
        localStorage.setItem(MY_LIST_TODOS, JSON.stringify([todo]))
      }
    }

  const toggleIsDone = (todo: string) => {
    const list = getItems()

    const itemToUpdate = list.find((t: Todo) => t.todo === todo)

    removeItem(itemToUpdate)

    setItem({...itemToUpdate, done: !itemToUpdate.done})

  }
  const getItems = () => {
    const arrData = localStorage.getItem(MY_LIST_TODOS)
    const arr = arrData && JSON.parse(arrData)
    return arr  ?? []
  }

  const updateList = (listTodo: Todo[]) => {
    localStorage.setItem(MY_LIST_TODOS, JSON.stringify(listTodo))
    window.location.reload()
  }

  const removeItem = (todo: Todo) => {
    const arr = getItems()
    const auxArr = arr.filter((t: Todo) => t.todo !== todo.todo)


    updateList(auxArr)
  }

  return {
    setItem,
    getItems,
    removeItem,
    toggleIsDone
  }
}