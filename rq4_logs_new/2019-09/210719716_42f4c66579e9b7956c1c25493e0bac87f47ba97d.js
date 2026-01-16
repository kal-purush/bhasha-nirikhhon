import React, { Component } from 'react';
import { Form } from 'semantic-ui-react';

class TodoForm extends Component {
  state = { todo_name: '' }

  handleChange = (e) => {
    const { name, value } = e.target
    this.setState({ [name]: value })
  }

  handleSubmit = (e) => {
    e.preventDefault()
    // add the item
    this.props.add(this.state)
    // clean phase
    this.setState({ todo_name: '' })
  }

  render() {
    const { todo_name } = this.state
    return(
      <Form onSubmit={this.handleSubmit}>
        <Form.Input
          label='Todo'
          placeholder='add a todo'
          required
          name='todo_name'
          value={todo_name}
          onChange={this.handleChange}
        />
      </Form>
    )
  }
}

export default TodoForm;