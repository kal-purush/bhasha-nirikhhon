import React, { Component } from 'react';

class About extends Component {
  static async getInitialProps() {
    const example = await new Promise(resolve =>
      setTimeout(() => {
        resolve('final-value');
      }, 5000)
    );
    console.log('About getInitialProps resolved');
    return { example };
  }
  render() {
    return (
      <div>
        {this.props.example
          ? 'about page with ' + this.props.example
          : 'Loading...'}
      </div>
    );
  }
}

export default About;