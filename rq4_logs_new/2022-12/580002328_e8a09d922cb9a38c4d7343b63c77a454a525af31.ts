import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Routes, RouterModule } from '@angular/router';
import { AppModule } from './app.module';
import { SymptomManagementFormComponent } from './symptom-management-form/symptom-management-form.component';
import { AppComponent } from './app.component';
import { TestComponent } from './test/test.component';

const appRoutes: Routes = [
  { path: 'symptomManageForm', 
    component: SymptomManagementFormComponent 
  },
  {
    path: '',
    component:AppComponent
  },
  {
    path: 'test',
    component: TestComponent
  }
  // {
  //   path: 'what will show up on the URL aka the mapping',
  //   component: name of the component in angular formatted like above
  // }
];

@NgModule({
  imports: [
      RouterModule.forRoot(appRoutes)
  ],
  exports: [
      RouterModule
  ]
})
export class AppRoutingModule { }