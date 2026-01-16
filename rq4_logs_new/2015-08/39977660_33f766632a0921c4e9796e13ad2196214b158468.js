var React = require('react');
var Colors = require('./Details/Colors.react.js');

var Details = React.createClass({

  render: function() {
    console.log('Rendering Details class');
    var p = this.props.palette;
    return (
      <div className="details">
        <div className="details__colorId" >{"Color ID: " + p.id}</div>
        <div className="details__colorCategory" >{"Category: " + p.category}</div>
        <div className="details__userId" >{"User ID: " + p.user.id}</div>
        <div className="details__userName" >{"User Name: " + p.user.name}</div>
        <Colors palette={p} />
      </div>
    )
  }

});

module.exports = Details;