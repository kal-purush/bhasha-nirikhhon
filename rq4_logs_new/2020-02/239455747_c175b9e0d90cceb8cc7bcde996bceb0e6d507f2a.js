import React from 'react'

const Hello = (props) => {
  return (
    <div>
      <p>Hello {props.name}, you are {props.age} years old</p>
    </div>
  )
}

const App = () => {
  const now = new Date()
  const a = 10
  const b = 20

  return React.createElement(
    'div',
    null,
    React.createElement(
      'p', null, now.toString()
    ),
    React.createElement(
      'p', null, a, ' plus ', b, ' is ', a + b
    ),
    React.createElement(Hello, { name: 'George', age: 20 }),
    React.createElement(Hello, { name: 'Daisy', age: 22 })
  )
}

export default App;