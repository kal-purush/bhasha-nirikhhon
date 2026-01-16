import React, { Component } from 'react'
import ReactDOM from 'react-dom'
import CSSModules from 'react-css-modules'
import style from './Tables.scss'

export default class Tables extends Component {
  render() {
    return(
      <div>
        <div styleName="box"/>
        <p>OK.</p>
        <p>{this.props.name}</p>
      </div>
    )
  } 
}

export default CSSModules(Tables, style)