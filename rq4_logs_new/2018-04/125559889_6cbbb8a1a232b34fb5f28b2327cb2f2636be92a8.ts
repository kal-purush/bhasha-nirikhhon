/* eslint-disable no-undef */

class Timer {
  private interval: any
  private milliseconds: number = 0

  public start () {
    if (!this.interval) {
      console.log('Starting timer')
      // Using arrow function to get around the setInterval 'this' problem
      this.interval = setInterval(() => this.timer(), 1000)
    } else {
      console.log('Timer already running')
    }
  }

  public stop () {
    if (this.interval) {
      console.log('Stopping timer')
      clearInterval(this.interval)
      console.log('Total elapsed time:', this.milliseconds)

      // Clean up for next start
      this.interval = undefined
      this.milliseconds = 0
    } else {
      console.log('Timer not running')
    }
  }

  private timer () {
    this.milliseconds += 1
    console.log('Currently elapsed time:', this.milliseconds)
  }
}

export {Timer}