import React from 'react';
import ReactDOM from 'react-dom';
import autobind from 'autobind-decorator';


@autobind
class AddMessageButton extends React.Component {

  render() {
    return (
      <div className="add-message-button-internal">
        <p>Add Message</p>
      </div>
    )
  }
}

ReactDOM.render(<AddMessageButton />, document.querySelector('.button-container'));