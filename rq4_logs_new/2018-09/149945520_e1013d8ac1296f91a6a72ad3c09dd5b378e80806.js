import Vue from 'vue'
import App from './app.vue'

import './assets/styles/test.css'
import './assets/images/bg.jpg'

const root = document.createElement('div')
document.body.appendChild(root)

new Vue({
    render:(h)=>(App)
}).$mount(root)