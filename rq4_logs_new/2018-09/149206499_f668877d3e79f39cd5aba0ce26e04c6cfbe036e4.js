import React, { Component } from 'react';
import './Notch.css';

class Notch extends Component {

  state = {}

  constructor(props) {
    super(props);
    
  }
  
  render() {
    return (
      <div className="notch" style={{backgroundColor: this.props.color}}>
      </div>
    );
  }
}

export default Notch;