import React from "react";
import ReactDOM from "react-dom";
import "./styles.css";

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: ""
    };
    this.handleChange = this.handleChange.bind(this);
  }

  handleChange(event) {
    this.setState({
      name: event.target.value
    });
  }

  render() {
    return (
      <div className="App">
        <label>
          <p>请输入你的名字：</p>
          <input onChange={this.handleChange} />
        </label>
        <div class="name-block">
          <span>你好，{this.state.name ? this.state.name : "朋友"}！</span>
        </div>
      </div>
    );
  }
}

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);