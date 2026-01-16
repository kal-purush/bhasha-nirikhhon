'use strict';

import * as React from 'react';
import * as SVGInjector from 'svg-injector';
import PureRenderMixin from 'react-addons-pure-render-mixin';

function svgConfig (args: any): any {
  	return {
    	evalScripts: args.evalScripts || 'never',
    	each: args.callback || null
  	};
};

interface SVGProps{}
interface SVGState{}

export default class SVGComponent extends React.Component<SVGProps, SVGState> {
    mixins = [PureRenderMixin]
  	componentDidMount (){
    	return SVGInjector([this.refs.svg], svgConfig(this.props));
  	}

  	render () {
    	return React.createElement(
    		'img',
    		{
      			className: this.props.className,
      			ref: 'svg',
      			'data-src': this.props.path,
      			'data-fallback': this.props.fallbackPath
  			}
  		)
	}
};