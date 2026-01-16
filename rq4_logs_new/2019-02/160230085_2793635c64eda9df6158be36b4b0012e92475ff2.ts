import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { EffectsModule } from '@ngrx/effects';
import { StoreModule } from '@ngrx/store';
import {AuthStoreModule} from '../../modules/auth/store/auth.store.module';
import {DashboardStoreModule} from '../../modules/dashboard/store/dashboard.store.module';

@NgModule({
  imports: [
    CommonModule,
    AuthStoreModule,
    DashboardStoreModule,
    StoreModule.forRoot({}),
    EffectsModule.forRoot([])
  ],
  declarations: []
})
export class RootStoreModule {}