import React, { Component } from 'react';
import 'materialize-css/dist/css/materialize.min.css';
import { BrowserRouter } from 'react-router-dom';
import HomeRedirector from './pages/HomeRedirector';
import SignIn from './components/auth/AuthPage';



class App extends Component {
  state = {
    user: {},
  }
  // componentDidMount() {
  //   this.authListener();
  // }

  // authListener() {
  //   fire.auth().onAuthStateChanged((user) => {
  //     if (user) {
  //       this.setState({ user });
  //     } else {
  //       this.setState({ user: null });
  //     }
  //   })
  // }

  render() {
    return (
      <BrowserRouter>
        {this.state.user ? (<HomeRedirector />) : (<SignIn />)}
      </BrowserRouter>
    );
  }
}

export default App;