import React, {Component} from 'react';
import propTypes from 'prop-types';

class SetCSSVariables extends Component {

	static propTypes = {
		cssVariables: propTypes.object.isRequired,
		className: propTypes.string
	};

	componentWillReceiveProps(props) {
		Object.keys(props.cssVariables).forEach(function (key) {
			document.documentElement.style.setProperty(key, props.cssVariables[key]);
		});
	}

	render() {
		return (this.props.children);
	}
}

export default SetCSSVariables;