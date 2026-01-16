import React from "react";

class DeleteNote extends React.Component {
  render() {
    return (
      <div>
        <button
          className={this.props.className}
          onClick={() => this.props.delete(this.props.id)}
        >
          Delete Note
        </button>
      </div>
    );
  }
}

export default DeleteNote;