import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';

import { IonicModule } from '@ionic/angular';

import { CreateActivityPageRoutingModule } from './create-activity-routing.module';

import { CreateActivityPage } from './create-activity.page';
import {MaterialModule} from "../../material.module";

@NgModule({
    imports: [
        CommonModule,
        FormsModule,
        IonicModule,
        CreateActivityPageRoutingModule,
        MaterialModule,
        ReactiveFormsModule
    ],
  declarations: [CreateActivityPage]
})
export class CreateActivityPageModule {}