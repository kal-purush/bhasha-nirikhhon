import React, { Component } from 'react';
import ReactDOM from 'react-dom';
import '../index.css';
import BubbleLeft from './sub-components/BubbleLeft';


class MessageStream extends Component {
  render() {
    return (
      <div className="loginBox">
          <input type="email" placeholder="Email Address" />
          <input type="password" placeholder="Your Password" />
          <input type="checkbox" />I agree to the terms and conditions
      </div>
    );
  }
}

export default MessageStream;