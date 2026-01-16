import {NgModule} from '@angular/core';
import {RouterModule, Routes} from '@angular/router';
import {HomePageComponent} from "./components/home-page/home-page.component";
import {ErrorPageComponent} from "./error-page/error-page.component";

const routes: Routes = [
  { path: "", redirectTo:"/home", pathMatch: 'full'},
  { path: "home", component: HomePageComponent },
  { path: "error", component: ErrorPageComponent },
  { path: "**", redirectTo: "/error" },
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule],
})
export class AppRoutingModule {
}