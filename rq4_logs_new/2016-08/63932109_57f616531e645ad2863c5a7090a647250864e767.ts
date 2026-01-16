import { RouterConfig } from '@angular/router';
import { CheyouHubComponent } from './cheyou-hub.component';
import { CheyouDetailComponent } from './cheyou-detail.component';
import { CheyouBuyComponent } from './cheyou-buy.component';

export const routes = {
  path: 'cheyou',
  children: [
    {
      path: '', // content
      component: CheyouHubComponent
    },
    {
      path: 'detail',
      component: CheyouDetailComponent,
    },
    {
      path: 'buy',
      component: CheyouBuyComponent,
    },
  ]
};