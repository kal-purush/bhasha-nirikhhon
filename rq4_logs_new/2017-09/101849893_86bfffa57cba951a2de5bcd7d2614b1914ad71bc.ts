import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { HomePage } from './home';
import { RevmaxProvider } from '../../providers/revmax';
import { WooApiModule } from 'ng2woo';

const WooCommerceConfig = {
  url:   'http://revmax.twinspark.co', 
  consumerKey:    'ck_5ecf43a297b5341dfb68c4ba5f7e83db56125b19',
  consumerSecret:  'cs_6387cb6a55c87e8cd6223fbca39a92324dbfd013',
  wpAPI: true,
  version: 'wc/v2'
};
@NgModule({
  declarations: [HomePage],
  imports: [
    IonicPageModule.forChild(HomePage),
    WooApiModule.forRoot(WooCommerceConfig)
  ],
  providers: [
    RevmaxProvider,
    WooApiModule
  ]
})
export class HomePageModule { }