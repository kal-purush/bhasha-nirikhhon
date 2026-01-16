import { mount, createLocalVue } from '@vue/test-utils';
import EpKoulutustyyppiSelect from './EpKoulutustyyppiSelect.vue';
import Vue from 'vue';
import BootstrapVue from 'bootstrap-vue';

Vue.use(BootstrapVue);

describe('EpKoulutustyyppiSelect', () => {
  const localVue = createLocalVue();

  it('Renders', () => {
    const wrapper = mount(EpButton, {
      propsData: {
      },
      localVue,
    });
  });

});
