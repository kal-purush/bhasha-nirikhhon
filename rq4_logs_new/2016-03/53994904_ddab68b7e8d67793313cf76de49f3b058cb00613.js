var React = require('react');

var MyComponent = React.createClass({
	render: function(){
		return (
			<div className="my-component" onClick={this.props.onClick}>Hello World</div>
		);
	}
});
module.exports = MyComponent;