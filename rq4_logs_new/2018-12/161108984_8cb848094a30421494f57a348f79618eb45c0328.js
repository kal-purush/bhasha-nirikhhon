import Vue from 'vue'
import Router from 'vue-router'
import Logon from '@/pages/Logon'
import Firstpage from '@/pages/Firstpage'
import Regist from '@/pages/Regist'
import first from '@/components/first'
import customer from '@/components/customer'

import project from '@/components/children/project'
import message from '@/components/children/message'

Vue.use(Router)

export default new Router({
  routes: [
    
    {
      path: '/logon',
      name: 'Logon',
      component: Logon,
      children:[
      ]
    },
    {
      path: '/index',
      name: 'Firstpage',
      component: Firstpage,
      children:[
        {
          path:'first',
          component:first,
          children:[{path:'project',component:project}]
        },
        {
          path:'customer',
          component:customer,
          children:[{path:'message',component:message}]
        }
      ]
    },
    {
      path: '/regist',
      name: 'Regist',
      component: Regist,
      children:[
      ]
    },
  ]
})