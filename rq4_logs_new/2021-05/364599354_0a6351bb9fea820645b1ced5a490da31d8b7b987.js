import React, { Component } from 'react'


class AddForm extends Component {
    render() {
      return (
        <div>
          <form onSubmit={this.props.onSubmit} noValidate autoComplete="off">
            <input
              name="name"
              type="text"
              placeholder="Name"
            />
            <input
              name="calories"
              type="text"
              placeholder="calories"
            />
            <button type="submit" >
              Add food
            </button>
          </form>
        </div>
      );
    }
  }
  

export default AddForm