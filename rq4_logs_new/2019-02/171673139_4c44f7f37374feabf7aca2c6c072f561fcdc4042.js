import React, { Component } from 'react'

export default class List extends Component {
  render() {
    return (
      <div>
        {this.props.platanos.map((platano) => {
          return <p>{platano.name}</p>
        })}
      </div>
    )
  }
}