import BootstrapVue from 'bootstrap-vue';
import EpTreeNavibar from './EpTreeNavibar.vue';
import Vue from 'vue';
import VueCompositionApi, { reactive, computed } from '@vue/composition-api';
import VueI18n from 'vue-i18n';
import { FlattenedNodeDto, EpTreeNavibarStore } from './EpTreeNavibarStore';
import { Kaannos } from '../../plugins/kaannos';
import { Kielet } from '../../stores/kieli';
import { mount, createLocalVue, RouterLinkStub } from '@vue/test-utils';


describe('EpTreeNavibar component', () => {
  const localVue = createLocalVue();
  localVue.use(VueI18n);
  Kielet.install(localVue);
  localVue.use(new Kaannos());
  localVue.use(BootstrapVue);

  test('Mounts', async () => {
    const wrapper = mount(EpTreeNavibar, {
      localVue,
      propsData: {
        store: new EpTreeNavibarStore(computed(() => navipuu)),
      },
      stubs: {
        fas: '<div />',
        RouterLink: RouterLinkStub,
      },
    });

    expect(wrapper.html()).toContain('Päätason tekstikappale');
    expect(wrapper.html()).toContain('Tutkinnon muodostuminen');
  });
});


const navipuu = {
  "id" : null,
  "label" : null,
  "type" : "root",
  "meta" : { },
  "children" : [ {
    "id" : 630,
    "label" : null,
    "type" : "tutkinnonosat",
    "meta" : { },
    "children" : [ {
      "id" : 700,
      "label" : {
        "_tunniste" : "138790d9-44af-43db-84f4-cab3614ac0de",
        "fi" : "Tosa",
        "_id" : "645"
      },
      "type" : "tutkinnonosaviite",
      "meta" : {
        "koodi" : null,
        "laajuus" : 5.00
      },
      "children" : [ ]
    } ]
  }, {
    "id" : 601,
    "label" : {
      "_tunniste" : "70c994d3-8313-4dc8-8455-f1ed0602482d",
      "fi" : "Tutkinnon muodostuminen",
      "_id" : "640"
    },
    "type" : "muodostuminen",
    "meta" : { },
    "children" : [ ]
  }, {
    "id" : 770,
    "label" : {
      "_tunniste" : "a97c7fb1-82be-4077-854b-b0a1afa33681",
      "fi" : "Päätason tekstikappale",
      "_id" : "791"
    },
    "type" : "viite",
    "meta" : { },
    "children" : [ {
      "id" : 771,
      "label" : {
        "_tunniste" : "814f5703-b022-43e9-89c3-0a12a121b161",
        "fi" : "Aliotsikko",
        "_id" : "792"
      },
      "type" : "viite",
      "meta" : { },
      "children" : [ ]
    } ]
  } ]
};