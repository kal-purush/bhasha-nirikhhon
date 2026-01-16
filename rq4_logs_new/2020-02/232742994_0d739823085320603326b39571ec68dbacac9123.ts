import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { delay, map } from 'rxjs/operators';
import { environment } from '../../../environments/environment';


@Injectable()
export class DashboardService {

  constructor(private http: HttpClient) {}

  public baseUrl = environment.baseUrl;

  vehicleDataByAssign(){
    return this.http.get(this.baseUrl+"/vehicles/vehicleAssignNotAssignCount");
  }

  loadVehicleStatusByCount(){
    return this.http.get(this.baseUrl+"/vehicles/vehicleStatusWithCount");
  }

  loadExpenseType(){
    return this.http.get(this.baseUrl+"/expenses/types");
  }


  addService(data){
    return this.http.post(this.baseUrl+"/service/add", data);
  }

  addExpense(data){
    return this.http.post(this.baseUrl+"/expenses/add", data);
  }


  loadVehicleServices(page){
    return this.http.get(this.baseUrl+"/service/vehicle_services?page="+page);
  }

  loadVehicleExpenses(page){
    return this.http.get(this.baseUrl+"/expenses/vehicle_expenses?page="+page);
  }

  loadVehicleIssueStatus(){
    return this.http.get(this.baseUrl+"/expenses/vehicleIssueStatus");
  }
}