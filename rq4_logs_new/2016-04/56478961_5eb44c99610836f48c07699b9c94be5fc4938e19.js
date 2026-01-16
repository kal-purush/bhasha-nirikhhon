import React from 'react';

export default React.createClass({


  render: function(){
    let tableHeadNodes = this.props.headers.map(function(title){
        return (
          <th> {title.name}</th>

        )
      });

    return (
      <table>
        <thead>
            {tableHeadNodes}
        </thead>
      </table>
    )
  }
});