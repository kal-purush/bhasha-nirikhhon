import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { PrescriptionsComponent } from './components/prescriptions/prescriptions.component';
import { BookingComponent } from './components/booking/booking.component';

const routes: Routes = [
  // {path : 'prescriptions', component: PrescriptionsComponent },
  // {path : 'booking', component: BookingComponent }
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}