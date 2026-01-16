import { createLocalVue, mount } from '@vue/test-utils';
import EpKoodistoSelect from './EpKoodistoSelect.vue';
import BootstrapVue from 'bootstrap-vue';
import { KoodistoSelectStore } from './KoodistoSelectStore';
import { Page } from '../../tyypit';
import { KoodistoKoodiDto } from '../../api/eperusteet';
import _ from 'lodash';

describe('EpKoodistoSelect component', () => {
  const localVue = createLocalVue();
  localVue.use(BootstrapVue);

  const store = new KoodistoSelectStore({
    query: jest.fn(async () => {
      return {
        data: [{
          koodiUri: 'koodiuri1',
          koodiArvo: 'koodiarvo1',
        }, {
          koodiUri: 'koodiuri2',
          koodiArvo: 'koodiarvo2',
        }, {
          koodiUri: 'koodiuri3',
          koodiArvo: 'koodiarvo3',
        }] as KoodistoKoodiDto[],
        sivu: 0,
        sivukoko: 3,
        sivuja: 1,
      } as Page<KoodistoKoodiDto>;
    }),
  });

  function mountWrapper(props: any, methods: any) {
    return mount(EpKoodistoSelect,
      {
        propsData: {
          store: store,
          ...props,
        },
        scopedSlots: {
          default: '<b-button id="open" @click="props.open">lisaa koodi</b-button>',
        },
        listeners: {
          add: methods.lisaaKoodit,
        },
        attachToDocument: true,
        localVue,
        mocks: {
          $t: x => x,
          $kaanna: x => x,
          $sdt: x => x,
          $sd: x => x,
        },
        stubs: {
          fas: '<div />',
        },
      });
  }

  test('Renders', async () => {
    const wrapper = mountWrapper({
      multiple: false,
    }, {
      lisaaKoodit: (valittuKoodi) => {},
    });
    wrapper.find({ ref: 'editModal' }).setProps({ static: true });
    wrapper.find('#open').trigger('click');
    await localVue.nextTick();
    expect(wrapper.html()).toContain('koodiarvo1');
  });

  test('single row selected and returned', async () => {
    let koodi;
    const wrapper = mountWrapper({
      multiple: false,
    }, {
      lisaaKoodit: (valittuKoodi) => {
        koodi = valittuKoodi;
      },
    });
    wrapper.find({ ref: 'editModal' }).setProps({ static: true });
    wrapper.find('#open').trigger('click');
    await localVue.nextTick();

    wrapper.findAll('tr[role="row"]').at(1)
      .trigger('click');

    expect(koodi.uri).toBe('koodiuri1');
  });

  test('multiple rows selected and returned', async () => {
    let koodit;
    const wrapper = mountWrapper({
      multiple: true,
    }, {
      lisaaKoodit: (valittuKoodi) => {
        koodit = valittuKoodi;
      },
    });
    wrapper.find({ ref: 'editModal' }).setProps({ static: true });
    wrapper.find('#open').trigger('click');
    await localVue.nextTick();

    wrapper.findAll('tr[role="row"]').at(1)
      .trigger('click');
    wrapper.findAll('tr[role="row"]').at(2)
      .trigger('click');

    expect(_.size(koodit)).toBe(0);
    wrapper.find('footer.modal-footer').find('.btn-primary')
      .trigger('click');

    expect(_.size(koodit)).toBe(2);
    expect(koodit[0].uri).toBe('koodiuri1');
    expect(koodit[1].uri).toBe('koodiuri2');
  });
});