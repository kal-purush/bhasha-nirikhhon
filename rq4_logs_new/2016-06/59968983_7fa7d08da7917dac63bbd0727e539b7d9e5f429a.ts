import {
    provideRouter,
    RouterConfig
} from "@angular/router";

import { DashboardComponent } from "../components/dashboard/dashboard.component";
import { HeroRoutes } from "./hero/hero.routes";

export const routes: RouterConfig = [
    {
        path: "",
        component: DashboardComponent
    },
    {
        path: "dashboard",
        component: DashboardComponent
    },
    ...HeroRoutes
];

export const APP_ROUTER_PROVIDERS = [
    provideRouter(routes)
];