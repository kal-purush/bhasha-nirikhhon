import React from 'react';
import starWarsLogo from '../starWarsLogo.png';
import './Loading.css';
import '../App.css';

export default class Loading extends React.Component {
  render() {
    if (this.props.loading === true) {
      return(
        <section className="Loading">
          <img src={starWarsLogo} className="App-logo" alt="logo" />
        </section>
        )
    }
    return (
      <section className="Loading">
          
      </section>
    )
  }
}