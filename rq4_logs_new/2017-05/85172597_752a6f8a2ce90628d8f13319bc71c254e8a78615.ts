import {NgModule} from '@angular/core';
import {MdButtonModule, MdCardModule, MdCheckboxModule, MdDialogModule, MdInputModule} from '@angular/material';

@NgModule({
  imports: [
    MdInputModule,
    MdCardModule,
    MdDialogModule,
    MdButtonModule,
    MdCheckboxModule
  ],
  exports: [
    MdInputModule,
    MdCardModule,
    MdDialogModule,
    MdButtonModule,
    MdCheckboxModule
  ]
})
export class AppMaterialModule {
}