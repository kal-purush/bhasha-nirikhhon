import { Routes } from '@angular/router';
import { PATH_HOME, PATH_DETAIL } from "./consts";
import { HomepageComponent } from "./homepage/homepage.component";
import { DetailComponent } from './detail/detail.component';

export const ROUTES: Routes = [
    { path: PATH_HOME, component: HomepageComponent },
    { path: PATH_DETAIL, component: DetailComponent }

];