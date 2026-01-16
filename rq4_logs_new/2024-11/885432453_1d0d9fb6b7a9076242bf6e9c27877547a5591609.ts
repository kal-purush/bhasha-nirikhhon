import { createApp } from 'vue'
import './style.css'
import App from './App.vue'

// 如果只想导入css变量
import 'element-plus/theme-chalk/dark/css-vars.css'
import 'element-plus/dist/index.css'
import ElementPlus from "element-plus";
import { ElCollapseTransition } from "element-plus";

// 注册 ECharts 组件
createApp(App)
  .use(ElementPlus)
  .component(ElCollapseTransition.name as string, ElCollapseTransition)
  .mount('#app')