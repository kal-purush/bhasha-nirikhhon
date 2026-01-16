import Vue from 'vue'
import VueRouter from 'vue-router';
import VueResource from 'vue-resource';
import Root from './Root.vue'

Vue.use(VueRouter);
Vue.use(VueResource);

const routes = [{ path: '/', component: Root }];
const router = new VueRouter({ routes, mode: 'history' });

new Vue({
  router,
  template: `<router-view class="view"></router-view>`
}).$mount('#root');

// new Vue({
//   el: '#root',
//   render: h => h(Root)
// })