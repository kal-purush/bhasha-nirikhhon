import { NgModule } from '@angular/core';
import { RouterModule, Routes } from '@angular/router';
import { EnterResultsComponent } from './enter-results/enter-results.component';
import { ViewResultsComponent } from './view-results/view-results.component';

const routes: Routes = [
	{ path: 'enter', component: EnterResultsComponent },
	{ path:'view', component: ViewResultsComponent}
]


@NgModule({
	imports: [ RouterModule.forRoot(routes) ],
	exports: [
		RouterModule
	],
	declarations: []
})
export class AppRoutingModule { }