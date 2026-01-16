
import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
//import { RouterModule } from '@angular/router';
import { AppComponent } from './app.component';
import { EmployeeComponent } from './employee/emp.component';
import { VehicleComponent } from './vehicle/vehicle.component';
import { VehicleDetailComponent } from './vehicle/vehicleDetail.component';
import { AppRoutes, routing, appRoutingProviders } from './app.routes';

@NgModule({
  
  imports: [
    BrowserModule,
    routing
  ],
  
  declarations: [
    AppComponent,
    EmployeeComponent,
    VehicleComponent,
    VehicleDetailComponent
  ],
  
  providers: [
    appRoutingProviders
  ],
  
  bootstrap: [AppComponent]
  
})
export class AppModule {
}