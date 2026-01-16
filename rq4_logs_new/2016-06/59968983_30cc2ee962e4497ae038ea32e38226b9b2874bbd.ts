import { bootstrap } from "@angular/platform-browser-dynamic";
import { provideRouter } from "@ngrx/router";
import { Routes } from "@ngrx/router";

import { AppComponent } from "./components/app/app.component";
import { HeroesComponent } from "./components/hero/hero.component";
import { DashboardComponent } from "./components/dashboard/dashboard.component";
import { HeroDetailComponent } from "./components/detail/detail.component";

const routes: Routes = [
    {
        path: "/",
        component: DashboardComponent
    },
    {
        path: "/heroes",
        component: HeroesComponent
    },
    {
        path: "/dashboard",
        component: DashboardComponent
    },
    {
        path: "/detail/:id",
        component: HeroDetailComponent
    }
];

bootstrap(AppComponent, [
    provideRouter(routes)
]);