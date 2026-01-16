import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { MatCardModule } from '@angular/material/card';

import { MatButtonModule } from '@angular/material/button';
import { UserItemComponent } from './user-item.component';
import { AvatarModule } from '../avatar/avatar.module';

@NgModule({
  imports: [
    CommonModule,
    MatCardModule,
    MatButtonModule,
    AvatarModule
  ],
  declarations: [
    UserItemComponent
  ],
  exports: [
    UserItemComponent
  ]
})

export class UserItemModule {
}