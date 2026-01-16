import React from 'react';

import moment from 'moment-timezone';

import Japan from "./Japan";
import Usa from "./Usa";
import France from "./France";
import NewZealand from "./NewZealand";

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      name: "React",
      showHideJapan: false,
      showHideUSA: false,
      showHideFrance: false,
      showHideNewZealand: false,
    };
    this.hideComponent = this.hideComponent.bind(this);

  }

  hideComponent(name) {
    console.log(name);
    switch (name) {
      case "showHideJapan":
        this.setState({ showHideJapan: !this.state.showHideJapan });
        break;
      case "showHideUSA":
        this.setState({ showHideUSA: !this.state.showHideUSA });
        break;
      case "showHideFrance":
        this.setState({ showHideFrance: !this.state.showHideFrance });
        break;
        case "showHideNewZealand":
          this.setState({ showHideNewZealand: !this.state.showHideNewZealand });
          break;
      default: this.setState(name = "React");

    }
  }


  render() {
    console.log(moment.tz.names());
    const { showHideJapan, showHideUSA, showHideFrance, showHideNewZealand } = this.state;
    return (
      <div>
          <select multiple={true} >
            
            <option onClick={() => this.hideComponent("showHideJapan")}>Japan</option>
            <option onClick={() => this.hideComponent("showHideUSA")}>USA</option>
            <option onClick={() => this.hideComponent("showHideFrance")}>France</option>
            <option onClick={() => this.hideComponent("showHideNewZealand")}>New Zealand</option>
          </select>

          {showHideJapan && < Japan />}
          {showHideUSA && < Usa />}
          {showHideFrance && < France />}
          {showHideNewZealand && < NewZealand />}
      </div>
    );
  }
}

export default App;