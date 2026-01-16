/**
 * Created by hao.wang on 2017-4-10.
 */
import Vue from 'vue'
import _ from 'lodash'
import * as axios from 'axios'
// import vueFilter from 'vue-filter'
/**
 * app初始化，注入一些插件，vue原型属性和方法
 */
export function appInitInject (store, router) {


  // 注入原型工具类方法
  Vue.prototype.$axios = axios
  Vue.prototype.$_ = _

  Vue.prototype.$t = function (msg) { return msg }

  //
  // axios.defaults.baseURL = constants.ENV_CONFIG.apiBaseUrl
  axios.defaults.headers.post['Content-Type'] = 'application/json'
  axios.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded'

  //
  // Vue.mixin({
  //   mounted () {
  //     this.$$el = $(this.$el)
  //   }
  // })

  // Vue.prototype.$msgbox = MessageBox
  // Vue.prototype.$alert = MessageBox.alert
  // Vue.prototype.$confirm = MessageBox.confirm
  // Vue.prototype.$prompt = MessageBox.prompt

  // router.beforeEach((to, from, next) => {
  //   if (location.pathname === entry.app) {
  //     if (store.getters[types.GETTERS.isLogged] && store.getters[types.GETTERS.isRoleSelected]) {
  //       location.href = entry.app_study_evaluate
  //       return
  //     }
  //   }
  //   next()
  // })
  // 首页已登录 进入主页

  // initWxPreivew()
}




// export function registerFilters () {
//
//   Vue.use(vueFilter)
//
// }

