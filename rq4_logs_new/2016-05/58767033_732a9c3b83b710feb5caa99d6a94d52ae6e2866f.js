import React, { Component } from 'react';
import { connect } from 'react-redux';

export default function(ComposedComponent) {
  class Authentication extends Component {

    render() {
      return (
        <h3>require auth</h3>
      )
    }
  }