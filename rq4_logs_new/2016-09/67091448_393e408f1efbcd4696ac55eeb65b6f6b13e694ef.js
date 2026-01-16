import moment from 'moment'
import React, { Component } from 'react';
import { Modal, Button } from 'react-bootstrap';
import { TextField, RaisedButton, FontIcon, ListItem } from 'material-ui';

const iconStyles = {
  marginRight: 24,
  float: 'right'
};
const style = {
  margin: 12
};

export default class TodoItem extends Component {
  constructor(){
     super();

     this.state = {
       edit: false,
       todoText: '',
       todoId: ''
     }
     this._edit = this._edit.bind(this)
     this._delete = this._delete.bind(this)
     this._editMode = this._editMode.bind(this)
     this._onInputChange = this._onInputChange.bind(this)
   }
  _onInputChange(e){
    this.setState({todoText: e.target.value})
  }
  _editMode(id, task){
    this.setState({
      edit: true,
      todoText: task,
      todoId: id
    })
  }
  _delete(id){
    this.props.deleteTodo(id)
  }
  _edit(e) {
    e.preventDefault();
    let id = this.state.todoId
    let text = this.state.todoText;

    this.props.editTodo(id, text)
    this.setState({edit: false})
   }
  render(){
    let { id, task, time } = this.props.todo

    if(this.state.edit){
      return (
        <form onSubmit={this._edit}>
          <TextField
           floatingLabelText="Edit Task"
           floatingLabelFixed={false}
           onChange={this._onInputChange}
           value={this.state.todoText}
            />
          <RaisedButton label="Edit" type="submit" backgroundColor='#FF5252' labelColor='#FFFFFF' style={style}  />
        </form>
      )
    } else {
      return (
        <ListItem key={id} onDoubleClick={this._delete.bind(null, id)}>
        {moment(time).format('L')} : &nbsp;
        {task}
        <FontIcon className="material-icons" style={iconStyles} onClick={this._editMode.bind(null, id, task)}>edit</FontIcon>
        </ListItem>
      )
    }
  }
}