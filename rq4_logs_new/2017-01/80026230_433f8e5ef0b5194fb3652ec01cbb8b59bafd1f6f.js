import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import './compiler.css';
class App extends Component {
  render() {
    return (
      <div className="App">
        <div className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h2>Welcome to HTML to React</h2>
        </div>
        <p className="App-intro">
          To get started, paste your HTML inside the input at left.
        </p>
        <Compiler/>
      </div>
    );
  }
}

class Compiler extends Component {
  constructor()
  {
    super();
    this.state = {
      err : '',
      input: '/* add your html here*/',
      output: '',

    }
  }

  update(e)
  {

    let code = e.target.value;
    try {
      this.setState({
        output: window.Babel
                  .transform(code, {presets: ['es2015','react']})
                  .code,
        err: ''


      })
    } catch (error) {
      this.setState({err: error.message})
    }
  }
  render() {
    return (
      <div>
        <header>
          {this.state.err}
        </header>
        <div className="container">
          <textarea 
            onChange={this.update.bind(this)}
            defaultValue={this.state.input}
          ></textarea>
          <pre>{this.state.output}</pre>
        </div>
      </div>
    );
  }
}

export default App;