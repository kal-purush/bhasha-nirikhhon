import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { CatalogComponent } from './features/catalog/catalog.component';

const routes: Routes = [
  {
    path: '',
    component: CatalogComponent
  },
  {
    path: 'cart',
    loadComponent: () =>
      import('./features/catalog/catalog.component').then(// тут буде щлях до компоненту корзини
        (m) => m.CatalogComponent// тут буде компонент корзини
      ),
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}