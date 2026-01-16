import React, { Component } from 'react';
import { Link } from 'react-router';
import './messages.css';

class Messages extends Component {
  render() {
    return (
      <chat-messages>
        <h5><Link to="/">Home</Link></h5>
      </chat-messages>
    );
  }
}

export default Messages;