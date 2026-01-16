import React, { Component } from 'react'
import { Link } from 'react-router-dom'

export default class History extends Component {
  render() {
    return (
      <div>
        <h1>Notre histoire</h1>
        <Link to="/"> Retour à l'accueil </Link>
      </div>
    )
  }
}