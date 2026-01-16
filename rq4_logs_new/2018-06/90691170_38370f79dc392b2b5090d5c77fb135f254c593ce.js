import React, { Component } from 'react';

class App extends Component {
  constructor(props) {
    super(props)
    this.state = {
      loggedIn: false,
      loading: false,
      newUserForm: false,
      errorMessage: '',
    };
  }

  render() {
    return (
      <div className="App">
        <header className="App-header">
          <h1 className="App-title">Welcome to Passavr</h1>
        </header>
        <p className="App-intro">
          Keep all your passwords secure in one spot
        </p>
      </div>
    );
  }
}

export default App;