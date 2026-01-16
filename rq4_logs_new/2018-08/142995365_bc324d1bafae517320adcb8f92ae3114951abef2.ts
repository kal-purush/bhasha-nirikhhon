import { html, PolymerElement } from "@polymer/polymer/polymer-element.js";
import { customElement } from "../../src/index";
const assert = chai.assert;
const isRegistered = function(name) {
    return document.createElement(name).constructor !== HTMLElement;
};
suite("custom element fixture", () => {
    const fixtureElementName = "polymer-decorators-fixture";
    test("register element", () => {
        @customElement("custom-element")
        class Component extends PolymerElement {
            static get template() {
                return html`<h1 id="header">MyComponent</h1>`;
            }
        }
        let container: HTMLDivElement = fixture(fixtureElementName);
        let component: Component = <any>document.createElement("custom-element");
        container.appendChild(<any>component);
        assert(isRegistered("custom-element") == true);
    });
});