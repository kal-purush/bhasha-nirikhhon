import Vue from 'vue';
import TransactionChart from './TransactionChart';

Vue.config.delimiters = ['[[', ']]'];

Vue.component('transaction-chart', {
  extends: TransactionChart,
});

new Vue({
  el: '#vue-chart-container',
});