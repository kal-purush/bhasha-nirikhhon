
/// <reference path="../node_modules/immutable/dist/immutable.d.ts" />
/// <reference path="../node_modules/typescript/lib/lib.es6.d.ts" />

/// <reference path="node/node.d.ts" />
/// <reference path="express/express.d.ts" />
/// <reference path="mime/mime.d.ts" />
/// <reference path="serve-static/serve-static.d.ts" />
/// <reference path="compression/compression.d.ts" />
/// <reference path="body-parser/body-parser.d.ts" />
/// <reference path="react/react.d.ts" />
/// <reference path="assertion-error/assertion-error.d.ts" />
/// <reference path="chai/chai.d.ts" />
/// <reference path="mocha/mocha.d.ts" />
/// <reference path="react-dom/react-dom.d.ts" />
/// <reference path="react-redux/react-redux.d.ts" />
/// <reference path="redux/redux.d.ts" />
/// <reference path="redux-thunk/redux-thunk.d.ts" />
/// <reference path="socket.io/socket.io.d.ts" />
/// <reference path="socket.io-client/socket.io-client.d.ts" />
/// <reference path="chai-as-promised/chai-as-promised.d.ts" />
/// <reference path="promises-a-plus/promises-a-plus.d.ts" />
/// <reference path="form-data/form-data.d.ts" />
/// <reference path="request/request.d.ts" />

declare module 'keymirror' {export default {}}
declare module 'waste-categories' {

	interface Bin {
		type: string;
		path: string;
		infos?: string[];
	}

	interface Dictionary {
	    [key: string]: Bin[];
	}

	var dictionary: Dictionary;
	
	export default dictionary;
}

declare module 'react-addons-pure-render-mixin' {export default {} }
declare module 'react-inlinesvg' {
	var svgClass: __React.ComponentClass<any>;
	export = svgClass;
}

declare module 'svg-injector' {
	function SVGInjector(elements: any[], options: any, done?: any): void;
	export = SVGInjector;
}

