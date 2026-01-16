import { mount, createLocalVue } from "@vue/test-utils";
import Vue from "vue";
import Vuetify from "vuetify";
import VueRouter from "vue-router";
import Component from "@/views/NotFound.vue";

const localVue = createLocalVue();
const router = new VueRouter();

localVue.use(VueRouter);

Vue.use(Vuetify);

describe("Authentication.vue", () => {
  const setup = () => {
    return mount(Component, {
      localVue,
      router,
    });
  };

  it("renders correctly", () => {
    const wrapper = setup();
    expect(wrapper).toMatchSnapshot();
  });

  it("contains text about 404", () => {
    const wrapper = setup();
    expect(wrapper.text()).toContain("We don't know about this page yet");
  });

  it("redirect to root path after clicking button", () => {
    const wrapper = setup();
    const spy = jest.spyOn(router, "push");

    wrapper.find(".v-btn").trigger("click");

    expect(spy).toHaveBeenCalledWith(
      expect.objectContaining({
        path: "/",
      }),
    );
    spy.mockClear();
  });
});