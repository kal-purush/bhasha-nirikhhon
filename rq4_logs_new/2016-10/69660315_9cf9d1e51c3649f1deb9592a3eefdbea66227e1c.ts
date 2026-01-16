import { DOM, createElement } from "react";
import { shallow } from "enzyme";

import { Carousel } from "../Carousel";

describe("Carousel component", () => {
    const render = () => shallow(createElement(Carousel, { message: "Hello" }));
    const renderUganda = () => shallow(createElement(Carousel, { message: "Hello Uganda" }));

    it("should render message", () => {
        const output = render();
        expect(output).toBeElement(DOM.span({ className: "carousel" }, "Hello"));
    });

    it("should render message Hello Uganda", () => {
        const output = renderUganda();
        expect(output).toBeElement(DOM.span({ className: "carousel" }, "Hello Uganda"));
    });
});