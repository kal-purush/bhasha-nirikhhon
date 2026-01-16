import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { Routes, RouterModule } from '../../../node_modules/@angular/router';
import { HomeComponent } from '../home/home.component';
import { SignupComponent } from '../signup/signup.component';
import { LoginComponent } from '../login/login.component';
import { AuthService as AuthGuard } from '../core/auth.service';
import { StockInsertComponent } from '../medi/stock-insert/stock-insert.component';
import { CompanyInsertComponent } from '../medi/company-insert/company-insert.component';
import { StockShowComponent } from '../medi/stock-show/stock-show.component';
import { CompanyInsertComponent2 } from '../medi/company-insert2/company-insert2.component';
import { CompanyShowComponent } from '../medi/company-show/company-show.component';
import { InvoiceInsertComponent } from '../medi/Invoice-insert/Invoice-insert.component';

const applicationRoutes:Routes = [
  {
    path:'',
    component:LoginComponent,
    data: {
      isBackButtonEnabled: false
    }
  },
  {
    path:'signup',
    component:SignupComponent,
    data: {
      isBackButtonEnabled: false
    }
  },
  {
    path:'home',
    component:HomeComponent,
    canActivate : [AuthGuard],
    data: {
      isBackButtonEnabled: true
    }
  },
  {
    path:'addStock',
    component:StockInsertComponent,
    canActivate : [AuthGuard],
    data: {
      isBackButtonEnabled: true
    }
  },
  {
    path:'addMedicine',
    component:CompanyInsertComponent,
    canActivate : [AuthGuard],
    data: {
      isBackButtonEnabled: true
    }
  },
  {
    path:'addMedicine2',
    component:CompanyInsertComponent2,
    canActivate : [AuthGuard],
    data: {
      isBackButtonEnabled: true
    }
  },
  {
    path:'viewStock',
    component:StockShowComponent,
    canActivate : [AuthGuard],
    data: {
      isBackButtonEnabled: true
    }
  },
  {
    path:'viewMedicines',
    component:CompanyShowComponent,
    canActivate : [AuthGuard],
    data: {
      isBackButtonEnabled: true
    }
  },
  {
    path:'addInvoice',
    component:InvoiceInsertComponent,
    canActivate : [AuthGuard],
    data: {
      isBackButtonEnabled: true
    }
  },

];

@NgModule({
  imports: [
    CommonModule,RouterModule.forRoot(applicationRoutes)
  ],
  exports:[RouterModule],
  declarations: []
})
export class AppRoutingModule { }