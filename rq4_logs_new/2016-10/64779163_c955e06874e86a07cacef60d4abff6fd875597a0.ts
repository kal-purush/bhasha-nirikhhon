import {NgModule} from '@angular/core';
import {RouterTestingModule} from '@angular/router/testing';
import {DummyComponent} from './dummy.component';

@NgModule({
  imports: [
    RouterTestingModule.withRoutes([
      {path: '', component: DummyComponent},
      {path: ':year/:month', component: DummyComponent}
    ])
  ],
  declarations: [
    DummyComponent
  ],
  exports: [
    RouterTestingModule
  ]
})
export class CalendarTestModule {
}