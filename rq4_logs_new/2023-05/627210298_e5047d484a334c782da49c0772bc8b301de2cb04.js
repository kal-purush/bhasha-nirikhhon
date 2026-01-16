import {createApp, h} from 'vue/dist/vue.esm-bundler';
import App from './App.vue'
import 'ant-design-vue/dist/antd.css';
import { store } from './store/store'


var app = createApp({
    name: 'App',
    render: () => {
        return <App/>
    }
})

app.use(store)
app.mount('#app-id')
