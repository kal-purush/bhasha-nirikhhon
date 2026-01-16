import { NgModule }                   from '@angular/core';
import { RouterModule, Routes }       from '@angular/router';
import { ResortsComponent }           from './resorts/resorts.component';
import { SheetViewComponent }         from './sheet-view/sheet-view.component';
import { SheetjsComponent }           from './sheetjs/sheetjs.component';
import { CourseChartComponent }       from './course-chart/course-chart.component';
// import { AppComponent }               from './app.component';

const routes: Routes = [
  { path: '', redirectTo: 'upload', pathMatch: 'full' },
  { path: 'upload', component: SheetjsComponent },
  { path: 'data', component: SheetViewComponent },
  { path: 'resorts', component: ResortsComponent },
  { path: 'courses', component: CourseChartComponent }
  { path: 'map', component: SheetjsComponent }
];

@NgModule({
  imports: [ RouterModule.forRoot(routes) ],
  exports: [ RouterModule ]
})
export class AppRoutingModule { }