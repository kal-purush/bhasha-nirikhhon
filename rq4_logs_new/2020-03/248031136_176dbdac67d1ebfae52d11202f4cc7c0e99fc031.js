import React, { Component } from 'react';

class Clock extends Component {
  state = { date: new Date() }

  componentDidMount() {
    this.ticker = setInterval( () => this.tick(), 1000)
  }

  componentWillUnmount() {
    clearInterval(this.ticker)
  }

  tick = () => {
    this.setState({ date: new Date() })
  }

  render() {
    const { date } = this.state
    return(
      <p>
        {
          date.toLocaleTimeString()
        }
      </p>
    )
  }
}

export default Clock;