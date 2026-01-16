/**
 * Created by satish on 09/08/16.
 */

import {provideRouter, RouterConfig } from '@angular/router';
import {ReaderComponent} from "./reader.component";
import {ContributorComponent} from "./contributor.component";
import {FavoritesComponent} from "./favorites.component";
import {InputComponent} from "./input.component";
import {ProfileComponent} from "./profile.component";
import {MailerComponent} from "./mailer.component";

const routes: RouterConfig = [
    {path: '', redirectTo: 'reader', pathMatch : 'full'},

    {path : 'reader', component : ReaderComponent,children: [
        { path: '', redirectTo: 'favorites', pathMatch: 'full' },
        { path: 'favorites', component: FavoritesComponent },
        { path: 'profile',component:ProfileComponent}
    ]}
];

export const APP_ROUTER_PROVIDERS = [
    provideRouter(routes)
];