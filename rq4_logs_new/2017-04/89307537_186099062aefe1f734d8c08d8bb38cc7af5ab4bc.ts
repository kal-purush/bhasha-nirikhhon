import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HeroesComponent } from './heroes.component';
import { HeroesDetailsComponent } from './heroes-details/heroes-details.component';

const heroesRoutes: Routes = [
    { path: 'heroes', component: HeroesComponent, children: [
        { path: ':id', component: HeroesDetailsComponent }
    ] }
];

@NgModule({
    imports: [
        RouterModule.forChild(heroesRoutes)
    ],
    exports: [
        RouterModule
    ]
})
export class HeroRoutingModule {}