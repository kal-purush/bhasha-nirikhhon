import React from "react";
import { mount, shallow } from 'enzyme';
import App from "./App";

it("should render the app", () => {
    const wrapper = shallow(<App />);
    expect(wrapper.exists()).toBe(true);
});

it("when we enter a keyword, it should display some suggestions", () => {
    const wrapper = mount(<App />);
    wrapper.find("input[type='text']").simulate("change", {
        target: { value: 'achieve' }
    })
    expect(wrapper.find("li")).toHaveLength(3);
})