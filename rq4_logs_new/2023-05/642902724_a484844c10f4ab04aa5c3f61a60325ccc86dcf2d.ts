import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { HomeComponent } from './home/home.component';
import { LoginComponent } from './login/login.component';
import { SignupComponent } from './signup/signup.component';
import { CartegoryComponent } from './cartegory/cartegory.component';
import { UserComponent } from './user/user.component';
import { OrderComponent } from './order/order.component';
import { ContactComponent } from './contact/contact.component';
const routes: Routes = [
  { path: 'login', component: LoginComponent},
  { path: 'home', component: HomeComponent },
  { path: 'signup', component: SignupComponent },
  { path: 'success', component: SignupComponent },
  { path: 'category', component: CartegoryComponent},
  { path: 'user', component: UserComponent},
  { path: 'order', component: OrderComponent},
  { path: 'contact', component: ContactComponent},
  { path: '', redirectTo: '/login', pathMatch: 'full' },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }