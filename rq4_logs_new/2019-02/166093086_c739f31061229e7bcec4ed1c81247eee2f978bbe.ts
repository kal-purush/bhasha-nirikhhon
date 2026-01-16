import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Routes, RouterModule } from '@angular/router';
import { AdminComponent } from './view/admin/admin.component';
import { LoginComponent } from './components/login/login.component';
import { SharedModule } from '../../shared/shared.module';
import { WrapperComponent } from './components/wrapper/wrapper.component';
import { NgbTabsetModule } from '@ng-bootstrap/ng-bootstrap';
import { FormsModule } from '@angular/forms';
import { ItemService } from 'src/app/services/item.service';
import { AddItemFormComponent } from './components/item-form/add-item-form.component';
import { EditItemFormComponent } from './components/edit-item-form/edit-item-form.component';
import { DeleteItemFormComponent } from './components/delete-item-form/delete-item-form.component';

const routes: Routes = [
  { 
    path: '', component: AdminComponent, pathMatch: 'full', data: {title: 'Administration'}
  }
];

@NgModule({
  declarations: [AdminComponent, LoginComponent, WrapperComponent, AddItemFormComponent, EditItemFormComponent, DeleteItemFormComponent],
  imports: [
    CommonModule,
    RouterModule.forChild(routes),
    SharedModule,
    NgbTabsetModule,
    FormsModule
  ],
  providers: [ItemService]
})
export class AdminModule { }