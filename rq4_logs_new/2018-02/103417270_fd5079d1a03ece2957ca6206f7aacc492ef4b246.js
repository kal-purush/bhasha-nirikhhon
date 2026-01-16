export const addTodo = el => {
  console.log(el)
  // add new todo only when user press Enter
  // if input value is empty, don't add new todo
  // if (el.keyCode !== 13 || !el.target.value) return
  // return {
  //   type: 'ADD_TODO',
  //   name: el.target.value
  // }
}

// set item as done with one click
export const switchStatus = el => {
  const items = this.state.items
  items[el.target.id].done = !items[el.target.id].done

  this.setState({ items })
}

export const setValue = el => {
  this.setState({ value: el.target.value })
}

// remove item with double click
export const removeItem = el => {
  this.setState(this.state.items.splice(el.target.id, 1))
}