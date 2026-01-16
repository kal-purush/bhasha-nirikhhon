import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import {LoginComponent} from "./componets/login/login.component";
import {SignupComponent} from "./componets/signup/signup.component";
import {MainComponent} from "./componets/main/main.component";
import {PortfolioComponent} from "./componets/portfolio/portfolio.component";
import {ChooseProjectComponent} from "./componets/choose-project/choose-project.component";
import {MainAdminComponent} from "./componets/main-admin/main-admin.component";


//Об'єкти у масиві визначають шлях та компонент, який буде відображено при відповідному шляху в інтерфейсі користувача.
const routes: Routes = [
  {path:'login', component:LoginComponent},
  {path:'signup', component:SignupComponent},
  {path:'main',component:MainComponent},
  {path:'Portfolio', component:PortfolioComponent},
  {path:'chooseProject', component:ChooseProjectComponent},
  {path:'main-admin', component:MainAdminComponent}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }