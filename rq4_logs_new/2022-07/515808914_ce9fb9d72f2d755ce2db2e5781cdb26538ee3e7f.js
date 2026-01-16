import React from "react";

export default class IssueRow extends React.Component {
  render() {
    const style = this.props.rowStyle;
    return (
      // <div> Placeholder for IssueRow</div>
      <tr>
        <td style={style}>{this.props.issue_id}</td>
        {/* one way to pass teh data fom Parent to child  */}
        {/* <td style ={style}>{this.props.issue_title}</td> one */}
        {/* otehr wayto pass tehdata to  */}
      </tr>
    );
  }
}