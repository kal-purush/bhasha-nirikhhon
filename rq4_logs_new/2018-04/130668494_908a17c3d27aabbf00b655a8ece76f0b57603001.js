import Vue from 'vue'
import App from './App.vue'
import VueRouter from 'vue-router'
import Vuex from 'vuex'
import {store} from './store/store'

// lazy-loading
const Home = () => import('./components/Home.vue')
const Users = () => import('./components/Users.vue')
const Hello = () => import('./components/Hello.vue')
const Done = () => import('./components/TodosDone.vue')
const Open = () => import('./components/TodosOpen.vue')

Vue.use(Vuex);
Vue.use(VueRouter);
 
const routes = [
	{ path: '/', component: Home},
	{ path: '/users/:teamId', component: Users},
	{ path: '/hello', component: Hello},
	{ path: '/open', component: Open},
	{ path: '/done', component: Done}
];

const router = new VueRouter({
  	mode: 'history',
	routes
});

new Vue({
  el: '#app',
  router,
  store,
  render: h => h(App),
        created() {
	        // console.info(this.$store.getters['a/name']);
	        console.info("CREATED", this.$store.getters.count);
        }
})