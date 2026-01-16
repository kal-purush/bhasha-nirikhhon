import { ComponentA, ComponentB, ComponentC, ComponentD } from './components';



export const routes = [
  { path: '', redirectTo: 'a/1', pathMatch: 'full' },
  { path: 'a', children: [
      { path: ':id', component: ComponentA, children: [
          { path: '', redirectTo: 'b', pathMatch: 'full' },
          { path: 'b', component: ComponentB },
          { path: 'c', component: ComponentC },
          { path: ':id', outlet: 'aux', component: ComponentD }
      ] }
    ]}
      
]