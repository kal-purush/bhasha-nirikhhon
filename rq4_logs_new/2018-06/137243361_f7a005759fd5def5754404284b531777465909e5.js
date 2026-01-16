import React, {Component} from 'react';
import {Popup} from 'semantic-ui-react'

class EventPopup extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    console.log(this.props);
    return (
      <div>
        <Popup content={this.props.start}/>
      </div>
    )
  }
}

export default Popup