import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import axios from 'axios';

class App extends Component {
  getPosts() {
    axios.get('http://localhost:3000/posts').then((response) => console.log(response.data));
  }

  postPost() {
    axios
        .post('http://localhost:3000/posts', { "id": Math.random(), "title": "json-server", "author": "typicode" })
        .then((response) => console.log(response.data));
  }

  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <p>
            Edit <code>src/App.js</code> and save to reload.
          </p>
          <a
            className="App-link"
            rel="noopener noreferrer"
            onClick={this.getPosts.bind(this)}
          >
            Get posts and print in console
          </a>
          <a
              className="App-link"
              rel="noopener noreferrer"
              onClick={this.postPost.bind(this)}
          >
            Post one post and print posted post in console
          </a>
        </header>
      </div>
    );
  }
}

export default App;