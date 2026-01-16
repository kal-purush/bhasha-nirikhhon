/// <reference path="Definitions/react-0.13.0.d.ts"/>
import React = require('react');
import Entity = require('./Entity');

interface Props {
	entities: { art: string[] }[]
}

class Scene extends React.Component<Props, any, any> {
	render() {
		var divs = [];

		this.props.entities.forEach((e) => {
			divs.push(React.DOM.div({ style: null }, React.createElement(Entity, e)));
		});

		return React.DOM.div(null,
			divs
		);
	}
}

var contract = <React.ComponentClass<Props, any, any>>Scene;
export = contract;