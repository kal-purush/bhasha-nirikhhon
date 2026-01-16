import Vue from 'vue'
import Vuetify from 'vuetify'
import Vuelidate from 'vuelidate'
import { createLocalVue, mount, shallowMount, Wrapper } from '@vue/test-utils'
import StatusService from '../../src/services/status.services'
import SbcSystemAlert from '@/components/SbcSystemAlert.vue'
import Vuex from 'vuex'

Vue.use(Vuetify)
Vue.use(Vuelidate)
// suppress "avoid mutating a prop directly" warnings
// https://vue-test-utils.vuejs.org/api/config.html#silent
Vue.config.silent = true

let vuetify = new Vuetify({})

jest.mock('../../src/services/status.services')

describe('SbcSystemAlert.vue', () => {
  const $t = () => 'Payment service unavailable'
  const serviceData = [{
    serviceName: 'PAYBC',
    serviceNameDesc: 'Payment'
  }]

  it('Check service call with true', done => {
    let mockDetails = { 'currentStatus': 'True', 'nextUpTime': 0 }
    StatusService.getServiceStatus = jest.fn().mockResolvedValue(mockDetails)

    const wrapper = shallowMount(SbcSystemAlert, {
      propsData: { serviceData, statusURL: 'https://status-api-dev.pathfinder.gov.bc.ca/api/v1/' },
      mocks: { $t }
    })
    expect(wrapper.isVueInstance()).toBeTruthy()

    expect(StatusService.getServiceStatus).toBeCalled()

    expect(wrapper.props().serviceData).toBe(serviceData)
    expect(wrapper.props().statusURL).toBe('https://status-api-dev.pathfinder.gov.bc.ca/api/v1/')

    Vue.nextTick(async () => {
      expect(wrapper.vm.$data.isSbcSystemDown).toBeFalsy()
      expect(wrapper.vm.$data.alertMessage).toBe('')

      wrapper.destroy()
      done()
    })
  })

  it('Check service call with false', done => {
    let mockDetails = { 'currentStatus': 'False', 'nextUpTime': 0 }
    StatusService.getServiceStatus = jest.fn().mockResolvedValue(mockDetails)

    const wrapper = shallowMount(SbcSystemAlert, {
      propsData: { serviceData, statusURL: 'https://status-api-dev.pathfinder.gov.bc.ca/api/v1/' },
      mocks: { $t }
    })

    expect(StatusService.getServiceStatus).toBeCalled()
    Vue.nextTick(async () => {
      expect(wrapper.vm.$data.isSbcSystemDown).toBeTruthy()
      expect(wrapper.vm.$data.alertMessage).toBe('Payment service unavailable')

      wrapper.destroy()
      done()
    })
  })
})