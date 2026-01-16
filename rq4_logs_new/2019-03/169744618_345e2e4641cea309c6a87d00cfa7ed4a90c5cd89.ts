import { shallowMount, createLocalVue } from "@vue/test-utils";
import Vue from "vue";
import Component from "@/views/Authentication.vue";
import Signin from "@/components/Signin.vue";

describe("Authentication.vue", () => {
  const setup = () => {
    return shallowMount(Component);
  };

  it("renders correctly", () => {
    const wrapper = setup();
    expect(wrapper).toMatchSnapshot();
  });

  it("renders Signin component", () => {
    const wrapper = setup();
    const signin = wrapper.find(Signin);
    expect(signin.is(Signin)).toEqual(true);
  });
});