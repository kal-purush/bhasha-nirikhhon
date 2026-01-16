import React, { Component } from 'react';

class Todo extends Component{
  render(){ 
      return(
          <div className="todos">
              {this.props.value.map(i=>{if(i.area === this.props.area)
                  return <div className="todo" onClick={()=>{console.log(i)}}>{i.name}</div>
              })}
          </div>
      );
  }
}
export default Todo;