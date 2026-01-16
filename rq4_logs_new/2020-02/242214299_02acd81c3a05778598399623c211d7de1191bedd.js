import React, { Component } from "react";
import { render } from "react-dom";

const Stocks = ({ items }) => (
  <div className="table-responsive">
    <table className="table">
      <tbody>
        {items.map(item => (
          <tr>
            <td>{item["gsx$stock"]["$t"]}</td>
            <td>{item["gsx$price"]["$t"]}</td>
            <td>{item["gsx$quantity"]["$t"]}</td>
            <td>{item["gsx$date"]["$t"]}</td>
          </tr>
        ))}
      </tbody>
    </table>
  </div>
);
class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      data: []
    };
  }
  componentDidMount() {
    fetch(
      "https://spreadsheets.google.com/feeds/list/1Mo_tnsc0cIZEfRZhNfNxYKbqf_bkE-LLhl17xM5TEho/od6/public/values?alt=json"
    )
      .then(data => data.json())
      .then(jsonData => {
        this.setState({
          data: jsonData.feed.entry.reverse()
        });
      });
  }
  render() {
    if (this.state.data.length > 0) {
      console.log(this.state.data);
      return <Stocks items={this.state.data} />;
    }
    return <p>Loading....</p>;
  }
}

render(<App />, document.getElementById("root"));