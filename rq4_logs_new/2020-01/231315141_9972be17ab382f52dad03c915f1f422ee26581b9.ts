import { NgModule } from '@angular/core';
import { Routes, RouterModule, PreloadAllModules } from '@angular/router';

const routes: Routes = [
  {
    path: 'home',
    loadChildren: () =>
      import('./features/home/home.module').then(m => m.HomeModule)
  },
  // {
  //   path: 'about',
  //   loadChildren: () =>
  //     import('./features/about/about.module').then(m => m.AboutModule)
  // },
  // {
  //   path: 'experience',
  //   loadChildren: () =>
  //     import('./features/experience/experience.module').then(m => m.ExperienceModule)
  // },
  // {
  //   path: 'contacts',
  //   loadChildren: () =>
  //     import('./features/contacts/contacts.module').then(m => m.ContactsModule)
  // },
  {
    path: '',
    redirectTo: 'home',
    pathMatch: 'full'
  },
  {
    path: '**',
    redirectTo: 'home'
  }
];

@NgModule({
  // useHash supports github.io demo page, remove in your app
  imports: [
      RouterModule.forRoot(routes, {
        useHash: true,
        scrollPositionRestoration: 'enabled',
        preloadingStrategy: PreloadAllModules
    })
  ],
  exports: [RouterModule]
})
export class AppRoutingModule {}