import { mount, createLocalVue } from "@vue/test-utils";
import Vuetify from "vuetify";
import Vue from "vue";
import Vuex from "vuex";
import VueRouter from "vue-router";
import flushPromises from "flush-promises";
import Component from "@/components/Signin.vue";

const localVue = createLocalVue();

localVue.use(Vuex);
localVue.use(VueRouter);

Vue.use(Vuetify);

const router = new VueRouter();

let actions: any;
let store: any;
let $notify: any;
let wrapper: any;

describe("Signin.vue", () => {
  beforeEach(() => {
    actions = { authenticate: jest.fn() };
    $notify = jest.fn();
  });

  const setup = () => {
    store = new Vuex.Store({ actions });
    wrapper = mount(Component, {
      store,
      localVue,
      router,
      mocks: { $notify },
    });
  };

  it("renders correctly", () => {
    setup();
    expect(wrapper).toMatchSnapshot();
  });

  it("submits provided email and password", async () => {
    const email = "user@email.com";
    const password = "password";
    setup();

    await submitWithData({ email, password });

    expect(actions.authenticate).toHaveBeenCalledWith(expect.anything(), { email, password }, undefined);
  });

  it("redirect to root path after submitting credentials", async () => {
    const email = "user@email.com";
    const password = "password";
    setup();
    const spy = jest.spyOn(router, "push");

    await submitWithData({ email, password });

    expect(spy).toHaveBeenCalledWith("/");
    spy.mockClear();
  });

  it("notifies about error", async () => {
    actions.authenticate = jest.fn().mockRejectedValue(new Error("Action error"));
    setup();

    await submit();

    expect($notify).toHaveBeenCalledWith({
      text: "Can't sign in. Please make sure your login and password are correct",
      title: "Authentication",
      type: "error",
    });
  });
});

async function submitWithData({ email, password }: { email: string; password: string }) {
  wrapper.find("input[name='email']").setValue(email);
  wrapper.find("input[name='password']").setValue(password);
  await submit();
}

async function submit() {
  wrapper.find(".submit").trigger("click");
  await flushPromises();
}