import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';

import { AddColorComponent } from './addcolor.component';

const routes: Routes = [
    { path: '', component: AddColorComponent }
];

@NgModule({
    imports: [FormsModule,RouterModule.forChild(routes)],
    exports: [RouterModule]
})
export class AddColorRoutingModule { }