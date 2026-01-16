import React, { Component } from 'react';
import './Overview.css';

class Overview extends Component {
  progress () {
    return { width: (this.props.closed/this.props.total)*100 + '%' };
  }
  
  render () {
    return (
      <div className="Overview">
        <div className="BarContainer">
          <div className="BarFiller Back" style={this.progress()}></div>
          <div className="BarFiller Front" style={this.progress()}></div>
        </div>
      </div>
    )
  }
}

export default Overview;