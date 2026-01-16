import { shallowMount, createLocalVue } from "@vue/test-utils";
import Vuetify from "vuetify";
import Vue from "vue";
import Vuex from "vuex";
import VueRouter from "vue-router";
import Component from "@/App.vue";

const localVue = createLocalVue();

localVue.use(Vuex);
localVue.use(VueRouter);

Vue.use(Vuetify);

let actions: any;
let store: any;
let wrapper: any;

describe("App.vue", () => {
  beforeEach(() => {
    actions = { fetchToken: jest.fn() };
  });

  const setup = () => {
    store = new Vuex.Store({ actions });
    wrapper = shallowMount(Component, {
      store,
      localVue,
      stubs: ["notifications"],
    });
  };

  it("renders correctly", () => {
    setup();
    expect(wrapper).toMatchSnapshot();
  });

  it("fetches token on page load", async () => {
    setup();

    expect(actions.fetchToken).toHaveBeenCalled();
  });
});