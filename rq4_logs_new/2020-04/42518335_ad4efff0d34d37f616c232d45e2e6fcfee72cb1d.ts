import { createLocalVue, mount } from '@vue/test-utils';
import EpTekstikappaleLisays from './EpTekstikappaleLisays.vue';
import BootstrapVue from 'bootstrap-vue';
import _ from 'lodash';
import { mocks } from '@shared/utils/jestutils';

describe('EpKoodistoSelect component', () => {
  const localVue = createLocalVue();
  localVue.use(BootstrapVue);

  function mountWrapper(props: any, methods: any) {
    return mount(EpTekstikappaleLisays,
      {
        propsData: {
          ...props,
        },
        listeners: {
          save: methods.saveTekstikappale,
        },
        attachToDocument: true,
        localVue,
        mocks: mocks,
        stubs: {
          fas: '<div />',
        },
      });
  }

  test('Renders', async () => {
    const wrapper = mountWrapper({
      tekstikappaleet: [],
      paatasovalinta: true,
    }, {
      saveTekstikappale: (saveTekstikappale) => {},
    });

    wrapper.find({ ref: 'tekstikappalelisaysModal' }).setProps({ static: true });
    wrapper.find('#tekstikappalelisaysBtn').trigger('click');

    await localVue.nextTick();
    expect(wrapper.html()).toContain('lisaa-uusi-tekstikappale');
    expect(wrapper.html()).toContain('toisen-tekstikappaleen-alla');

    wrapper.setProps({ paatasovalinta: false });

    expect(wrapper.html()).not.toContain('toisen-tekstikappaleen-alla');
  });

  test('saving without main branch', async () => {
    let tekstikappale;
    const wrapper = mountWrapper({
      tekstikappaleet: ['tekstk1', 'tekstk2'],
      paatasovalinta: false,
    }, {
      saveTekstikappale: (otsikko, saveTekstikappale) => {
        tekstikappale = {
          otsikko,
          saveTekstikappale,
        };
      },
    });

    wrapper.find({ ref: 'tekstikappalelisaysModal' }).setProps({ static: true });
    wrapper.find('#tekstikappalelisaysBtn').trigger('click');
    await localVue.nextTick();

    expect(wrapper.findAll('button.btn-primary[disabled]')).toHaveLength(1);

    wrapper.find('input').setValue('otsikko1');

    expect(wrapper.findAll('button.btn-primary[disabled]')).toHaveLength(1);

    wrapper.findAll('option').at(1)
      .setSelected();

    expect(wrapper.findAll('button.btn-primary[disabled]')).toHaveLength(0);

    wrapper.find('button.btn-primary').trigger('click');

    expect(tekstikappale.otsikko).toEqual({ 'fi': 'otsikko1' });
    expect(tekstikappale.saveTekstikappale).toEqual('tekstk1');
  });

  test('saving with main branch', async () => {
    let tekstikappale;
    const wrapper = mountWrapper({
      tekstikappaleet: ['tekstk1', 'tekstk2'],
      paatasovalinta: true,
    }, {
      saveTekstikappale: (otsikko, saveTekstikappale) => {
        tekstikappale = {
          otsikko,
          saveTekstikappale,
        };
      },
    });

    wrapper.find({ ref: 'tekstikappalelisaysModal' }).setProps({ static: true });
    wrapper.find('#tekstikappalelisaysBtn').trigger('click');
    await localVue.nextTick();

    expect(wrapper.vm.$data.taso).toBe('paataso');
    expect(wrapper.findAll('button.btn-primary[disabled]')).toHaveLength(1);

    wrapper.findAll('input').at(0)
      .setValue('otsikko1');

    expect(wrapper.findAll('button.btn-primary[disabled]')).toHaveLength(0);

    wrapper.find('button.btn-primary').trigger('click');

    expect(tekstikappale.otsikko).toEqual({ 'fi': 'otsikko1' });
    expect(tekstikappale.saveTekstikappale).toEqual({});
  });
});