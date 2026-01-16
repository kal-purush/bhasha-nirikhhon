import App from '@/App.vue';
import { mount } from '@vue/test-utils';
import router from '@/router';
import { createStore } from 'vuex';

const store = createStore({
  getters: {
    'login/getUser'() {
      return {
        role: 'role',
      };
    },
  },
  mutations: {
    initialiseStore() {
      return {};
    },
  },
});

describe('App', () => {
  let wrapper;

  it('should do correct computed', () => {
    wrapper = mount(App, {
      global: {
        plugins: [router, store],
      },
      stubs: ['router-link'],
    });
    expect(wrapper.exists()).toBe(true);
  });
});