import { NgModule } from '@angular/core';
//import { CommonModule } from '@angular/common';
import { RouterModule, Routes } from '@angular/router';

import { HomeComponent } from '../components/home/home.component'
import { Level1Component } from '../components/level1/level1.component';
import { Level2Component } from '../components/level2/level2.component';
import { Level3Component } from '../components/level3/level3.component';
import { Level4Component } from '../components/level4/level4.component';
import { CorrectComponent } from '../components/correct/correct.component';
import { WrongComponent } from '../components/wrong/wrong.component';

const route: Routes = [
  { path:'home', component: HomeComponent},
  { path:'level1', component: Level1Component},
  { path:'level2', component: Level2Component},
  { path:'level3', component: Level3Component},
  { path:'level4', component: Level4Component},
  { path:'wrong', component: WrongComponent},
  { path:'correct', component: CorrectComponent},
  { path:'', pathMatch: 'full', redirectTo: '/home'},
]

@NgModule({
  declarations: [],
  imports: [
   // CommonModule,
    RouterModule.forRoot(route)
  ],
  exports: [
    RouterModule
  ],

})
export class AppRoutingModule { }