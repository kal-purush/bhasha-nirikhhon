import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Routes, RouterModule } from '@angular/router';
import { FormsModule } from '@angular/forms';
import { WalletService } from './wallet.service';
import { WalletComponent } from './wallet.component';

const routes: Routes = [
  { path: 'wallet/:userId', component: WalletComponent}
];

@NgModule({
  declarations: [
    WalletComponent
  ],
  imports: [
    CommonModule,
    FormsModule
  ],
  providers: [
    WalletService
  ]
})
export class WalletModule { }