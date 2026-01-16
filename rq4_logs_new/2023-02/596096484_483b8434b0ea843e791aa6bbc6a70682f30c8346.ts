// Import stylesheets
import './style.css';
import { style2 } from './style2.css';

import('redgin')
  .then(({ injectStyles }) => {

    // inject global styles
    injectStyles.push(
      `@import url("https://fonts.googleapis.com/icon?family=Material+Icons")`
    )
    injectStyles.push(style2)

    // load compon
    import('./app')

    // configure router
    customElements.whenDefined('router-slot').then(async () => {

      const routerSlot = document.querySelector('app-root').shadowRoot.querySelector('router-slot')

      routerSlot.add([
        {
          path: 'product',          
          component: () => import('./productList'),
        },       
        {
          path: '',          
          component: () => import('./productList'),
        },
      ])
    })    
  
  })
  .catch((err) => console.log(err))
