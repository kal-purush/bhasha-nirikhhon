import React from 'react';

import './../../templates/styles-main.css'    
import './style-counter.css' 


export default class Counter extends React.Component {
  static defaultProps = {
    initialvalue: 0,
  }

  state = {
    value: Number(this.props.initialvalue),
  }

  handleIncrement = () => {
    this.setState(state => {
      return {
        value: state.value + 1,
      }
    })
  }

  handleDecrement = () => {
    this.setState(state => {
      return {
        value: state.value - 1,
      }
    })
  }

  render() {
    return (
        
        <article>
            
            <strong style={{ flex: `1 1` }}>{this.state.value}</strong>
        
            <button onClick={this.handleDecrement} className="bttn">Decrease +</button>
        
            <button onClick={this.handleIncrement} className="bttn">Increase +</button>
    
        </article>

    )
  }
}