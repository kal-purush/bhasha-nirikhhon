import React, {Component} from 'react';

export default class Home extends Component {
  constructor() {
    super();
  }

  render() {
    return (
      <div>
        <h1>Home</h1>
        <h3 className='welcome'>
          Welcome to the Gif Playground. Pick a gif, add some stickers, and take a screenshot! The place where you can 'gif-ify' your imagination!
        </h3>
      </div>
    )
  }
}