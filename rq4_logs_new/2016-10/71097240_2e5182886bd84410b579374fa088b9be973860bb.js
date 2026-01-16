import React from 'react';
import {connect} from 'react-redux';

import {addTodo} from '../actions/todos';

@connect((store) => {
  return {
  };
})
export default class AddTodo extends React.Component {

  formSubmit(e) {
    e.preventDefault();
    this.props.dispatch(addTodo(this._input.value.trim()));
    this._input.value = '';
  }

  render() {
    return (
      <form onSubmit={this.formSubmit.bind(this)} >
        <input type='test' placeholder='Add todo' ref = {(c)=>this._input = c}></input>
        <button type='submit'>Add Todo</button>
      </form>
    );
  }
}