import React, { Component } from 'react';

class ErrorBoundary extends Component {
  state = {
    hasError: false,
    errorMessage: ''
  }
//btener el error , funcion ya determinada de react.
  componentDidCatch = (error, info) => {
    //Actualizamos el error
    this.setState({hasError: true, errorMessage: error });
  }

  render() {
    if (this.state.hasError) {
        return <h1>this.state.errorMessage</h1>
    } else {
      return this.props.children;
    }
  }
}

export default ErrorBoundary;