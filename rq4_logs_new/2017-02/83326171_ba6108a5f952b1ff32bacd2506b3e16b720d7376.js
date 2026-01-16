var React = require('react');

var App = React.createClass ({
	getInitialState: function() {
		return {
			count: this.props.initialCount
		}
	},
	_increment: function() {
		this.setState({
			count: this.state.count+1
		})
	},
	render: function() {
		var face = {
			color:'red',
			padding:'10px 10px',
			fontSize:'25px',
			lineHeight:'25px'
		}
		return (
			<div>
				<span>the count is:</span>
				<span onClick= {this._increment} style={face}>{this.state.count}</span>
			</div>
		)
	}
})

module.exports = App;