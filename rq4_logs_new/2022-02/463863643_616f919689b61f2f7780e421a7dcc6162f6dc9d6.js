import React, { Component } from "react";

class NewTodoForm extends Component {
  constructor(props) {
    super(props);
    this.state = {
      todoText: "",
    };
    this.handleSubmit = this.handleSubmit.bind(this);
    this.handleChange = this.handleChange.bind(this);
  }
  handleChange(e) {
    this.setState({
      todoText: e.target.value,
    });
  }
  handleSubmit(e) {
    e.preventDefault();
    let todo = {
      todoText: this.state.todoText,
    };
    this.props.onSubmit(todo);
    this.setState({
      todoText: "",
    });
  }
  render() {
    return (
      <form>
        <label htmlFor="todo_text">Enter text:</label>
        <input
          type="text"
          name="todo_text"
          id="todo_text"
          onChange={this.handleChange}
          value={this.state.todoText}
        />
        <button type="submit" onClick={this.handleSubmit}>
          Add item
        </button>
      </form>
    );
  }
}

export default NewTodoForm;