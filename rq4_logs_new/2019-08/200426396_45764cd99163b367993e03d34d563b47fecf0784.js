import { h } from "@stencil/core";
export class HueHeader {
    render() {
        return (h("div", { class: "hue-header" }, "HueHeader"));
    }
    static get is() { return "hue-header"; }
    static get originalStyleUrls() { return {
        "$": ["hue-header.scss"]
    }; }
    static get styleUrls() { return {
        "$": ["hue-header.css"]
    }; }
}