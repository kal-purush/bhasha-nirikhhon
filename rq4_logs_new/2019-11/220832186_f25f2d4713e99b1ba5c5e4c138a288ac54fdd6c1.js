import React, { Component } from 'react'

function ShowError({ error }) {
  return (
    <div>
      <h1>Error: ${error.toString()}</h1>
      <h2>Check console for more details and sourcemaps.</h2>
      <ul style={{ listStyleType: 'none', color: 'red' }}>
        {error.stack.split('\n').map(str => (
          <li key={str}>{str}</li>
        ))}
      </ul>
    </div>
  )
}

export default class ErrorBoundary extends Component {
  state = {
    error: null
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  render() {
    const { error } = this.state

    if (error) {
      return <ShowError error={error} />
    }

    return this.props.children
  }
}