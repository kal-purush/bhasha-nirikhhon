import React from "react";
import Child from "./Child";
import "./App.css";

class App extends React.Component {
  constructor() {
    super();
    console.log("constructor parent");
  }

  state = {
    name: "yunus",
    euro: 0,
  };
  componentWillMount() {
    console.log("componentWillMount parent");
  }

  componentDidMount() {
    console.log("componentDidMount parent");

    setTimeout(() => {
      this.setState({
        name: "kötü kenan",
      });
    }, 2000);
  }

  deneme = (e) => {
    this.setState({
      euro: e.target.value,
    });
  };

  shouldComponentUpdate(nextProps, nextState) {
    return nextState.euro % 10 === 0;
  }

  componentWillUpdate(nextProps, nextState) {
    console.log("componentWillUpdate", nextProps, nextState);
  }

  componentDidUpdate(prevProps, prevState) {
    console.log("componentDidUpdate", prevProps, prevState);
  }

  render() {
    console.log("render parent");
    return (
      <div className="App">
        <header className="App-header">
          <input name="euro" onChange={this.deneme} />
          her püskevit 10 euro. <br />
          <br />
          <br />
          {this.state.euro / 10} tane püskevit alabilin.
          <div>{this.state.name}</div>
          {""}
          <Child />
        </header>
      </div>
    );
  }
}

export default App;