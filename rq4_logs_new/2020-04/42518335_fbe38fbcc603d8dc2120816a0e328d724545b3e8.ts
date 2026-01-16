import { mount, createLocalVue } from '@vue/test-utils';
import EpPagination from './EpPagination.vue';
import { Kaannos } from '../../plugins/kaannos';
import VueI18n from 'vue-i18n';
import { findContaining } from '../../utils/jestutils';
import { Kielet } from '../../stores/kieli';
import Vue from 'vue';
import BootstrapVue from 'bootstrap-vue';

Vue.use(BootstrapVue);

describe('EpPagination', () => {
  const localVue = createLocalVue();
  localVue.use(VueI18n);
  Kielet.install(localVue);
  localVue.use(new Kaannos());

  test('Empty pagination', async () => {
    const wrapper = mount(EpPagination, {
      localVue,
      propsData: {
        value: 1,
        perPage: 10,
        totalRows: 0,
      },
      mocks: {
        $t: x => x,
      },
      stubs: {
        fas: '<div />',
      },
    });

    expect(wrapper.html()).toEqual('<div></div>');
  });

  test('Pagination with less than six pages', async () => {
    const wrapper = mount(EpPagination, {
      localVue,
      propsData: {
        value: 1,
        perPage: 10,
        totalRows: 49,
      },
      mocks: {
        $t: x => x,
      },
      stubs: {
        fas: '<div />',
      },
    });
    expect(wrapper.findAll('.link-container').length).toEqual(7);
    expect(findContaining(wrapper, '.link-container', '1')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '2')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '3')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '4')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '5')).toBeTruthy();
  });

  test('Pagination with more than six pages', async () => {
    const wrapper = mount(EpPagination, {
      localVue,
      propsData: {
        value: 5,
        perPage: 10,
        totalRows: 80,
      },
      mocks: {
        $t: x => x,
      },
      stubs: {
        fas: '<div />',
      },
    });

    expect(wrapper.findAll('.link-container').length).toEqual(9);
    expect(findContaining(wrapper, '.link-container', '1')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '3')).not.toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '4')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '5')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '6')).toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '7')).not.toBeTruthy();
    expect(findContaining(wrapper, '.link-container', '9')).toBeTruthy();
  });

});
