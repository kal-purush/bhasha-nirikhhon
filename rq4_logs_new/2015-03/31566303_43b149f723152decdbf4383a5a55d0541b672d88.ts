import fs = require('fs');

import marked = require('marked');
import path = require('path');
import react = require('react');

var react_tools = require('react-tools');

export function convertMarkdownToReactJs(content: string) {
	var jsx = marked(content.toString(), {});
	jsx = '<div>' + jsx + '</div>';
	var js = 'return ' + react_tools.transform(jsx);
	return js;
}

function requiredComponentNames(contentSource: string) {
	var componentRegex = /React.createElement\(([A-z][A-Za-z]+)/g
	var match = componentRegex.exec(contentSource);
	var componentNames: string[] = [];
	while (match != null) {
		var componentClass = match[1];
		componentNames.push(componentClass);
		match = componentRegex.exec(contentSource);
	}
	return componentNames;
}

function loadComponent(name: string, searchPaths: string[]): react.Component<any,any> {
	var component: react.Component<any,any>;

	searchPaths.some((searchPath) => {
		try {
			var componentPath = searchPath + '/' + name;

			// test that the referenced component exists
			fs.statSync(componentPath + '.js');

			component = <react.Component<any,any>>require(componentPath);
			return true;
		} catch (ex) {
			// if the component does not exist, try the next search path,
			// otherwise alert the caller
			if (ex.code !== 'ENOENT') {
				throw ex;
			}
			return false;
		}
	});

	if (component) {
		return component;
	} else {
		throw new Error(`Failed to load component ${name}`);
	}
}

// takes the JS source for an expression which returns a React Element
// and evaluates it to create a React component with a render() function
// which returns the result
export function reactComponentFromSource(renderSource: string, componentsDir: string) {
	var componentNames = requiredComponentNames(renderSource);
	var components: react.Component<any,any>[] = [];

	var componentSearchPaths = [
	  path.resolve(componentsDir),
	  path.resolve(path.resolve(__dirname) + '/components')
	];

	// load components required by the post
	componentNames.forEach((name) => {
		components.push(loadComponent(name, componentSearchPaths));
	});

	// create a semi-sandbox for running the post-generating
	// code
	var args = ['React'].concat(componentNames);
	var renderFunc = Function.apply({}, args.concat(renderSource));
	var postComponent = react.createClass({
		render: () => {
			return renderFunc.apply({}, [<any>react].concat(components))
		}
	});
	return postComponent;
}
