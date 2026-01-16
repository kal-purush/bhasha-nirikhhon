//es6语法从外部引入别的库都可以用这样的方式
import Vue from 'vue';
import hello01 from "./components/hello.vue";

//开启错误提示
Vue.config.debug = true;
new Vue({
	el:'#app',
	render: h => h(hello01)
}) 