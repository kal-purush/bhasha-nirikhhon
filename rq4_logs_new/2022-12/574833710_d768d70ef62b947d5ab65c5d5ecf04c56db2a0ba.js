import './App.css'
import { Header } from './components/Header'
import { Main } from './components/Main'
import { Footer } from './components/Footer'
import { useEffect, useState } from 'react'

function App() {
  const [todos, setTodos] = useState([])
  const [noneCompletedItemsCount, setNoneCompletedItemsCount] = useState(0)

  useEffect(() => {
    fetch('https://jsonplaceholder.typicode.com/todos')
      .then((response) => response.json())
      .then(setTodos)
  }, [])

  useEffect(() => {
    const uncompleted = todos.filter((todo) => !todo.completed)
    setNoneCompletedItemsCount(uncompleted.length)
  }, [todos])

  const appTitle = 'TodosApp'

  const addTodo = (title) => {
    const newTodos = todos.concat([{ id: Date.now(), title, completed: false }])
    setTodos(newTodos)
    // todos = [ ...todos, { id: Date.now(), title, completed: false } ]
  }

  const removeTodo = (todoToRemove) => {
    todos = todos.filter((currentTodo) => currentTodo.id !== todoToRemove.id)
    console.log(todos)
  }

  const markAsCompleted = () => {}

  const clearAllCompletedItems = () => {
    todos = todos.filter((currentTodo) => !currentTodo.completed)
    console.log(todos)
  }

  const toggleAllItems = (checkedValue) => {
    todos = todos.map((todo) => ({ ...todo, completed: checkedValue }))
    // todos = todos.map( todo => Object.assign({}, todo, {completed: checkedValue}));
    console.log(todos)
  }

  return (
    <section className="todoapp">
      <Header
        title={appTitle}
        onAddItem={addTodo}
        text="What needs to be done?"
      />
      <Main items={todos} onToggleAll={toggleAllItems} />
      <Footer
        itemLeftCount={noneCompletedItemsCount}
        onClearCompleted={clearAllCompletedItems}
      />
    </section>
  )
}

export default App