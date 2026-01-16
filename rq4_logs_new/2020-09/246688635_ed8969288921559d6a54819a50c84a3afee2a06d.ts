import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HolyGrailLayoutComponent } from './layout/holy-grail-layout/holy-grail-layout.component';
import { IndexComponent } from './pages/index/index.component';

const routes: Routes = [
  {
    path: '',
    component: HolyGrailLayoutComponent,
    children: [{ path: '', component: IndexComponent }],
  },

  { path: '**', redirectTo: '' },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}