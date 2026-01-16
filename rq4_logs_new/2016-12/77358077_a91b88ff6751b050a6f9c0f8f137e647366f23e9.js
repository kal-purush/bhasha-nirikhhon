import React, { Component } from 'react';
var uniqid = require('uniqid');

class AddProject extends Component {
  constructor(){
      super();
      this.state = {
        newProject:{}
      }
  }

  static defaultProps = {
    categories : ['Web Development', 'Mobile Development']
  }

  handleSubmit(e){
    e.preventDefault();
    if(this.refs.title.value === ''){
      alert("Title is required");
    }else{
      this.setState({
        newProject: {
          id: uniqid(),
          title: this.refs.title.value,
          desc: this.refs.category.value
        }
      }, function(){
        //console.log(this.state.newProject);
        this.props.addProject(this.state.newProject);
      })
    }
  }

  render() {
    let categoryOptions = this.props.categories.map(category => {
      return <option key={category} value={category}>{category}</option>
    });

    return (
      <div>
        <h3>Add Project</h3>
        <form onSubmit={this.handleSubmit.bind(this)}>
          <div>
            <label>Title : </label>
            <input type="text" ref="title" />
          </div>
          <br />
          <div>
          <label>Description : </label>
          <select ref="category"> {categoryOptions} </select>
          </div>
          <br />
          <div>
            <input type="submit" value="Submit" />
          </div>
        </form>
        <br />
        <hr />
      </div>
    );
  }
}

export default AddProject;