import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router'
import { AppComponent } from './app.component';
import { DemoComponent } from './demo/demo.component';
import { PresentComponent} from './present/present.component'

const routes: Routes = [
    {
        path: '',
        component: AppComponent
    },
    {
        path: 'demo',
        component: DemoComponent
    },
    {
        path: 'present',
        component: PresentComponent
    }
];

@NgModule({
  imports: [
    BrowserModule,
    RouterModule.forRoot(routes)
  ],
  exports: [
    RouterModule
  ]
})
export class AppRoutingModule { }