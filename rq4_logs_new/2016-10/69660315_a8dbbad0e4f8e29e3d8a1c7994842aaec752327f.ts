import { ShallowWrapper, shallow } from "enzyme";
import { DOM, createElement } from "react";

import { Carousel, CarouselProps, Image } from "../Carousel";

describe("Carousel", () => {
    const images: Image[] = [
        { url: "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7" },
        { url: "https://www.google.com/images/nav_logo242.png" }
    ];
    let wrapper: ShallowWrapper<CarouselProps, any>;
    beforeEach(() => wrapper = shallow(createElement(Carousel, { images })));

    it("should render carousel structure", () => {
        expect(DOM.div({ className: "carousel" }, DOM.div({ className: "carousel-inner" })));
    });

    it("should render carousel with 2 children", () => {
        expect(wrapper.find(".carousel-inner").children().length).toEqual(images.length);
    });

    it("should render carousel with the class 'carousel'", () => {
        expect(wrapper).toHaveClass("carousel");
    });

    it("should render carousel child to have class 'carousel-inner'", () => {
        expect(wrapper.children()).toHaveClass("carousel-inner");
    });

    it("first child should be active", () => {
        const children = wrapper.find(".carousel-inner").children();
        expect(children.first().prop("active")).toBe(true);
    });

    it("should have only one active child", () => {
        const children = wrapper.find(".carousel-inner").children();
        const activeChildren = children.filterWhere(c => c.prop("active") === true);
        expect(activeChildren.length).toBe(1);
    });
});