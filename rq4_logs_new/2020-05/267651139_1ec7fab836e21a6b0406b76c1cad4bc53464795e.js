import React, { Component } from 'react';
import logo from './logo.png';
import './App.css';
import firebase from 'firebase';
import StyledFirebaseAuth from 'react-firebaseui/StyledFirebaseAuth';

firebase.initializeApp({
  apiKey: 'AIzaSyAqFXlTECfuLjlCrSDXnPzQZM9dmgvya78',
  authDomain: 'hardware-eshop.firebaseapp.com'
})

class App extends Component {

  state = { isSignedIn: false }
  uiConfig = {
    signInFlow: "popup",
    signInOptions: [
      firebase.auth.EmailAuthProvider.PROVIDER_ID,
      firebase.auth.GoogleAuthProvider.PROVIDER_ID,
      firebase.auth.FacebookAuthProvider.PROVIDER_ID
    ],
    callbacks: {
      signInSuccess: () => false
    }
  }

  componentDidMount = () => {
    firebase.auth().onAuthStateChanged(user => {
      this.setState({ isSignedIn: !!user });
      console.log('Usuario =>', user);
    })
  }

  render() {
    return (
      <div className="App" >
        <header className="App-header" >
          <img src={logo} className="App-logo" alt="logo" />
          <p>¿Necesita una cuenta? <hr /> Regístrese ahora mismo y haga sus compras</p>
          {
            this.state.isSignedIn ? (
              <span>
                <h1>Bienvenido {firebase.auth().currentUser.displayName}</h1>
                <img alt='profile' src={firebase.auth().currentUser.photoURL} />
                <button onClick={() => firebase.auth().signOut()}>Salir</button>
              </span>
            ) : (
                <StyledFirebaseAuth
                  uiConfig={this.uiConfig}
                  firebaseAuth={firebase.auth()}
                />
              )
          }
        </header>
      </div >
    );
  }

}

export default App;