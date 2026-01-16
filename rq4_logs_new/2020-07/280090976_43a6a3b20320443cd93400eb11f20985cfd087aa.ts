import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { AccountComponent } from './account/account.component';
import { ProfileComponent } from './profile/profile.component';
import { PaymentParametersComponent } from './payment-parameters/payment-parameters.component';
import { TrainerRateGainComponent } from './trainer-rate-gain/trainer-rate-gain.component';
import { PurchaseHistoryComponent } from './purchase-history/purchase-history.component';
import { InvoiceDetailComponent } from './invoice-detail/invoice-detail.component';



@NgModule({
  declarations: [AccountComponent, ProfileComponent, PaymentParametersComponent, TrainerRateGainComponent, PurchaseHistoryComponent, InvoiceDetailComponent],
  imports: [
    CommonModule
  ]
})
export class AccountModule { }