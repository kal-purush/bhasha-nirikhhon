const jsdom = require('jsdom-global')();

import * as m from "mithril";
import { expect } from "chai";

import { PrismController as Prism } from "../../src/prism/PrismController";

describe('Prism Component', () => {

	it('should have code attribute', (done) => {
		const component: any = m(Prism as any, {
			code: "<div>Example</div>",
			key: 1,
			oncreate: () => {
				expect(component.state.attrs.code).to.not.be.undefined;
				done();
			}
		});

		m.render(document.body, component);
	});

	it('should be html language by default', (done) => {
		const component: any = m(Prism as any, {
			code: "<div>Example</div>",
			key: 2,
			oncreate: () => {
				const element = component.instance.dom.querySelector("code");
				const className = element.getAttribute("class");

				expect(className).to.equal("language-html");
				
				done();
			}
		});

		m.render(document.body, component);
	});

	it('should be custom language', (done) => {
		const component: any = m(Prism as any, {
			code: "console.log('example')",
			key: 3,
			language: "js",
			oncreate: () => {
				const element = component.instance.dom.querySelector("code");
				const className = element.getAttribute("class");

				expect(className).to.equal("language-js");
				expect(component.state.attrs.language).to.equal("js");
				
				done();
			}
		});

		m.render(document.body, component);
	});
});