import React, { Component } from 'react'

export default class App extends Component {

  a = 100;
  myref = React.createRef();

  render() {
    return (
      <div>
        <input ref={this.myref}/>
        <button onClick={ () => {
          console.log(this.myref.current.value)
        }}>按钮1</button>
      </div>
    )
  }

}