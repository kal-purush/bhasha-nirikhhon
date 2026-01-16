import modelExtend from 'dva-model-extend'
import * as expressService from '../services/express'
const { tracing } = expressService
import { pageModel } from './common'

export default modelExtend(pageModel, {
  namespace: 'express',

  state: {
    list: [],
    currentItem: {},
    modalVisible: false,
    modalType: '',
    pagination: {
      showSizeChanger: true,
      showQuickJumper: true,
      showTotal: total => `共 ${total} 条`,
      current: 1,
      total: null,
    },
  },

  effects: {
    *'tracing' ({ payload }, { call, put }) {
      const data = yield call(tracing, payload)
      if (data && data.success) {
        yield put({
          type: 'tracingSuccess',
          payload: {
            list: data.result.status.traces.reverse(),
          },
        })
      }
    },
  },

  reducers: {
    tracingSuccess (state, action) {
      console.log(action)
      return { ...state, ...action.payload }
    },

    showModal (state, action) {
      return { ...state, ...action.payload, modalVisible: true }
    },
    hideModal (state) {
      return { ...state, modalVisible: false }
    },

    setCurrentItem (state, { payload }) {
      const { currentItem } = payload
      return { ...state, currentItem }
    },
  },
})