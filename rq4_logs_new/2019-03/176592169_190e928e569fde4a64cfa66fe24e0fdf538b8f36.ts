import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { BrowserModule } from '@angular/platform-browser';
import { HttpModule } from '@angular/http';
import { RouterModule, Routes } from '@angular/router';
import { WrapperComponent } from './wrapper/wrapper.component';
import { UserBarInfoComponent } from '../page/user-bar-info/user-bar-info.component';
import { FilterComponent } from '../page/filter/filter.component';
import { ListComponent } from '../page/list/list.component';
import { UserImageComponent } from '../page/user-bar-info/user-image/user-image.component';
import { LinkToProfileComponent } from '../page/user-bar-info/link-to-profile/link-to-profile.component';
import { DropdownFilterComponent } from '../page/filter/dropdown-filter/dropdown-filter.component';
import { AccordionComponent } from '../page/list/accordion/accordion.component';
import { FilterTasksByPipe } from '../page/filter-tasks-by.pipe';
import { ItemComponent } from '../page/list/accordion/item/item.component';
import { ModalComponent } from '../page/modal/modal.component';

import { AccordionModule } from 'ngx-bootstrap/accordion';
import { BsDropdownModule } from 'ngx-bootstrap/dropdown';
import { ModalModule } from 'ngx-bootstrap/modal';

export const routes: Routes = [
  { path: '', redirectTo: 'profile', pathMatch: 'full' },
  { path: '', component: WrapperComponent },
];

@NgModule({
  declarations: [
    WrapperComponent,
    UserBarInfoComponent,
    FilterComponent,
    ListComponent,
    UserImageComponent,
    LinkToProfileComponent,
    DropdownFilterComponent,
    AccordionComponent,
    FilterTasksByPipe,
    ItemComponent,
    ModalComponent
  ],
  imports: [
    CommonModule,
    RouterModule,
    BrowserModule,
    HttpModule,
    BsDropdownModule.forRoot(),
    AccordionModule.forRoot(),
    ModalModule.forRoot()
  ],
  bootstrap: [WrapperComponent]
})
export class PageModule { }