import React from 'react';

export default class Message extends React.Component{

  render(){
    return (
        <div>
          <p>{this.props.messageText.from}: {this.props.messageText.text}</p>
        </div>
    );
  }
}