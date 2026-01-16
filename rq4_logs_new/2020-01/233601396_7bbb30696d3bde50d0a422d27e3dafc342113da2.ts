import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { QiuzPage } from './qiuz.page';
import { SetComponent } from './set/set.component';
import { TestComponent } from './test/test.component';

const routes: Routes = [
  {
    path: '',
    component: QiuzPage,
    children: [
      { path: '', redirectTo: 'set', pathMatch: 'prefix' },
      {
        path: 'set',
        component: SetComponent,
      },
      {
        path: 'test',
        component: TestComponent,
      },
    ]
  }
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule],
})
export class QiuzPageRoutingModule {}