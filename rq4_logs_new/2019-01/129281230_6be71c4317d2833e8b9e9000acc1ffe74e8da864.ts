import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { DebugComponent } from 'app/debug/debug.component';
import { HomeComponent } from 'app/home/home.component';
import { PlanningRoomComponent } from 'app/planning-room/planning-room.component';

const appRoutes: Routes = [ // routes from specific to general.. position matters!!!
    { path: 'home', component: HomeComponent },
    {
        path: 'planning/:key',
        component: PlanningRoomComponent
    },
    { path: 'debug', component: DebugComponent },
    { path: '', redirectTo: 'home', pathMatch: 'full' },
    { path: '**', redirectTo: 'home', pathMatch: 'full' }
];

@NgModule({
    imports: [
        RouterModule.forRoot(appRoutes)
    ],
    exports: [
        RouterModule
    ]
})

export class RouterCommonModule {

}