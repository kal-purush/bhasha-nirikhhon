import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { UserRegistrationFormComponent } from './user-registration-form/user-registration-form.component';
import { UserProfileDetailsComponent } from './user-profile-details/user-profile-details.component';

const routes: Routes = [
  { path: '', pathMatch: 'full', redirectTo: '/register' },
  { path: 'register', component: UserRegistrationFormComponent },
  {
    path: 'profile',
    component: UserProfileDetailsComponent,
  },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {}