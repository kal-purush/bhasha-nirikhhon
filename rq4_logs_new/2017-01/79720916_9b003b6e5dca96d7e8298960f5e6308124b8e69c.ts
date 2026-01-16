import { NgModule } from "@angular/core";
import { RouterModule, Routes } from "@angular/router";

import { AppComponent } from "./app.component";

const routes: Routes = [
    {
        path:'',
        redirectTo:'login',
        pathMatch:"full"
      },
      {
        path:'',
        component:AppComponent
      },
      {
        path: 'login',
        loadChildren: "app/login/login.module#LoginModule"
      }
];

@NgModule({
    imports: [RouterModule.forRoot(routes, {useHash: true})],
    exports: [RouterModule]
})

export class AppRoutingModule {}