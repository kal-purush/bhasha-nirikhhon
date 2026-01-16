import React, { Component } from "react";
import axios from "axios";

import "./App.css";

class App extends Component {
  constructor(props) {
    super(props);

    this.state = {
      quote: "Random quote for you <3",
    };
  }

  componentDidMount() {
    this.fetchQuote();
  }

  fetchQuote = () => {
    const url = "https://api.adviceslip.com/advice";

    axios
      .get(url)
      .then((res) => {
        const slipData = res.data.slip;
        const adviceData = slipData.advice;

        this.setState({
          quote: adviceData,
        });
      })
      .catch((err) => {
        console.log("🚀 ~ file: App.js ~ line 24 ~ App ~ err", err);
      });
  };

  render() {
    return (
      <div className="app">
        <div className="card">
          <h1 className="heading">{this.state.quote}</h1>

          <button className="button" onClick={this.fetchQuote}>
            <span>GIVE ME ADVICE!</span>
          </button>
        </div>
      </div>
    );
  }
}

export default App;