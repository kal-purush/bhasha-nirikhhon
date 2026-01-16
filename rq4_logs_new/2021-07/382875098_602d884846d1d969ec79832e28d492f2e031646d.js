import React, { Component } from 'react';
import { ToastContainer, toast } from 'react-toastify';
import { connect } from 'react-redux';
import { createTodo } from '../redux/actions/todoAction';

class CreateTodo extends Component {
  state = {
    name: '',
  };

  handleChange = (e) => {
    this.setState({ [e.target.name]: e.target.value });
  };

  addTodo = (e) => {
    e.preventDefault();
    if (this.state.name.trim() === '') {
      toast.error('Please enter todo');
    } else {
      const newTodo = {
        title: this.state.name,
        completed: false,
        userId: 123,
      };
      this.props.createTodo(newTodo);
    }
  };
  render() {
    return (
      <>
        <ToastContainer />
        <form onSubmit={this.addTodo}>
          <input
            type="text"
            name="name"
            placeholder="Enter The Todo"
            onChange={this.handleChange}
          />
          <input type="submit" value="Add Todo" />
        </form>
      </>
    );
  }
}

export default connect(null, { createTodo })(CreateTodo);