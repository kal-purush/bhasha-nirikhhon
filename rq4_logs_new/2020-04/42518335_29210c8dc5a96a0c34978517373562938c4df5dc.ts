import { mount, createLocalVue, RouterLinkStub } from '@vue/test-utils';
import { findContaining } from '../../utils/jestutils';
import EpKayttaja from './EpKayttaja.vue';
import { Kieli } from '../../tyypit';
import { Kaannos } from '../../plugins/kaannos';
import VueI18n from 'vue-i18n';
import { delay } from '../../utils/delay';
import { Kielet } from '../../stores/kieli';
import Vue from 'vue';
import BootstrapVue from 'bootstrap-vue';
import { KoodistoSelectStore } from '../EpKoodistoSelect/KoodistoSelectStore';
import VaatimusField from './VaatimusField.vue';


Vue.use(BootstrapVue);


describe('VaatimusField', () => {
  const localVue = createLocalVue();
  localVue.use(VueI18n);
  Kielet.install(localVue);
  localVue.use(new Kaannos());

  test('Text input', async () => {
    const editoitava = {
      query: jest.fn(async (query: string, sivu = 0) => koodit as any),
    };
    const koodisto = new KoodistoSelectStore(editoitava);

    const wrapper = mount(VaatimusField, {
      localVue,
      propsData: {
        koodisto,
        value: {
          koodi: null,
          vaatimus: {
            fi: 'teksti',
          },
        },
      },
      mocks: {
        $t: x => x,
      },
      stubs: {
        fas: '<div />',
      },
    });

    const input = wrapper.find('input').element as any;
    expect(input.value).toBe('teksti');
    wrapper.find('input').setValue('muuta');
    expect(wrapper.props().value.vaatimus.fi).toBe('muuta');
    await delay();
  });

  test('Renders koodi', async () => {
    const editoitava = {
      query: jest.fn(async (query: string, sivu = 0) => koodit as any),
    };
    const koodisto = new KoodistoSelectStore(editoitava);

    const wrapper = mount(VaatimusField, {
      localVue,
      attachToDocument: true,
      propsData: {
        koodisto,
        value: {
          koodi: {
            uri: 'ammattitaitovaatimukset_1234',
            arvo: '1234',
            nimi: {
              fi: 'valittu koodi',
            },
          },
        },
      },
      mocks: {
        $t: x => x,
      },
      stubs: {
        fas: '<div />',
      },
    });

    const input = wrapper.find('input').element as any;
    expect(input.value).toBe('valittu koodi (1234)');
    expect(input.disabled).toBeTruthy();
  });

  test('Autocompletion', async () => {
    const editoitava = {
      query: jest.fn(async (query: string, sivu = 0) => koodit as any),
    };
    const koodisto = new KoodistoSelectStore(editoitava);

    const wrapper = mount(VaatimusField, {
      localVue,
      attachToDocument: true,
      propsData: {
        koodisto,
        value: {
          koodi: null,
          vaatimus: {
            fi: 'teksti',
          },
        },
      },
      mocks: {
        $t: x => x,
      },
      stubs: {
        fas: '<div />',
      },
    });

    wrapper.find('input').setValue('test');
    wrapper.find('input').trigger('focus');
    await delay();
    expect(wrapper.html()).toContain('nimi 1234');
    wrapper.findAll('.item').at(0).trigger('click');
    await delay();
    expect(wrapper.html()).not.toContain('nimi 1234');
    expect(wrapper.emitted().input[0][0].nimi).toBeFalsy();
    expect(wrapper.emitted().input[0][0].koodi).toEqual(
      expect.objectContaining({
        uri: 'ammattitaitovaatimukset_1234',
        arvo: '1234',
      }));
  });

});

const koodit = {
  data: [{
    koodiUri: 'ammattitaitovaatimukset_1234',
    koodiArvo: '1234',
    versio: '1',
    metadata: [{
      nimi: 'nimi 1234',
      kieli: 'FI',
      kuvaus: 'kuvaus 1234'
    }],
    koodisto: {
      koodistoUri: 'ammattitaitovaatimukset',
      latestKoodistoVersio: null
    },
    voimassaAlkuPvm: 1576713600000,
    voimassaLoppuPvm: null
  }]
};