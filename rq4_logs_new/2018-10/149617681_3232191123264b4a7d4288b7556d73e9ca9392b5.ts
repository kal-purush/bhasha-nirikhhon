import { NgModule } from '@angular/core';
import { IonicPageModule } from 'ionic-angular';
import { CreateeventPage } from './createevent';
import { Ng4GeoautocompleteModule } from 'ng4-geoautocomplete';
@NgModule({
  declarations: [
    CreateeventPage,
  ],
  imports: [
    IonicPageModule.forChild(CreateeventPage),
    Ng4GeoautocompleteModule.forRoot(),
  ],
})
export class CreateeventPageModule {}