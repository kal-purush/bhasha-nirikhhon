import React from "react";
import "normalize.css";
import Select from "react-dropdown-select";



export class Drop extends React.Component {
  state = {
    options: []
  };

  componentDidMount() {
    this.setState({ options: [{label: "6:00 AM", value: "a"},{label: "6:30 AM", value: "b"},{label: "7:00 AM", value: "c"},{label: "7:30 AM", value: "d"}]});
  }

  render() {
    return (
      <div>
        <Select
          placeholder=""
          onChange={() => console.log(this.state.options)}
          values={[]} 
          options={this.state.options}
        />
      </div>
    );
  }
}

export default Drop;