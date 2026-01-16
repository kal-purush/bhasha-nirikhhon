import { Component, OnInit, ElementRef } from '@angular/core';
import { NbDialogService } from '@nebular/theme';
import { VehicleService } from '../../vehicles/vehicles.service';
import { VehicleExpenseService } from '../../vehicle-expense/vehicleexpense.service';
import { VehicleservService } from '../../vehicle-service/vehicleserv.service';
import { ActivatedRoute, Router } from '@angular/router';


@Component({
  selector: 'ngx-vehicle-expense-list',
  templateUrl: './reports-list.component.html',
  styleUrls: ['./reports-list.component.scss']
})
export class ReportsListComponent implements OnInit {

  public vehicleTypes = [];
  public vehicleDetails = [];
  public vehicleRegistrations = [];
  public selectedVehicleType;
  public selectedVehicleDetail;
  public selectedVehicleReg;
  public currentPage:String;
  keyword = 'name';

  public filterQueryString = "";

  public vehiclesList: any[] = [];
  public totalItems: any;
  public pager = {};
  public pageOfItems = [];
  public dropDownAction = false;


  constructor(private vehicleService: VehicleService, private vehicleservService: VehicleservService, private activeRoute: ActivatedRoute, private eRef: ElementRef, private router:Router, private dialogService: NbDialogService) { }

  ngOnInit() {
    this.loadVehiclesTypes();
    this.activeRoute.queryParams.subscribe(queryParams => {
      this.loadExpensesData(queryParams.page);
    });

  }

  selectEvent(item, typeofautoselect) {
    switch (typeofautoselect) {
      case "vehicletype":
        this.selectedVehicleType = item.id;
        this.filterQueryString += "vehicleType="+this.selectedVehicleType;
        this.loadVehicleDetails(item.id);
        break;
      case "vehicledetails":
        this.selectedVehicleDetail = item.id;
        this.filterQueryString += "&vehicleDetail="+this.selectedVehicleDetail;

        break;
      case "vehiclereg":
        this.selectedVehicleReg = item.name;
        this.filterQueryString += "&vehicleReg="+this.selectedVehicleReg;

      default:
        // this.selectedVehicleType = item.id;
    }
  }


  public loadVehicles(page?){
    let p = page || 1;
    this.vehicleService.loadAssignVehicles(p).subscribe((vehicleData:any)=>{
     this.vehiclesList = vehicleData.data;
     console.log(this.vehiclesList);
     
     this.totalItems, this.pageOfItems = vehicleData.data; 
     this.pager = vehicleData.page;
    //  this.pageOfItems = vehicleData.data;

    },(error)=>{

    });
  }


  public loadExpensesData(page?){

    let p = page || 1;
    this.vehicleservService.loadVehicleExpenses(p).subscribe((vehicleData:any)=>{
     this.vehiclesList = vehicleData.data;
     
     this.totalItems, this.pageOfItems = vehicleData.data; 
     this.pager = vehicleData.page;
    //  this.pageOfItems = vehicleData.data;

    },(error)=>{

    });

  }







  public loadVehiclesTypes(){
    this.vehicleService.loadVehiclesTypes().subscribe((vehicleType:any) => {
      let vehicleTypeData = vehicleType.data;
      vehicleTypeData.forEach((item,index) => {
        let tmpObj = {};
        tmpObj["id"] = item.vehicleTypeId;
        tmpObj["name"] = item.vehicleType;
        this.vehicleTypes.push(tmpObj);
      });
      // console.log(vehicleTypeData);
    });
  }


  public loadVehicleDetails(vehicleTypeId){
    this.vehicleService.loadVehicleDetails(vehicleTypeId).subscribe((vehicleDetails:any) => {
      let vehicleDetailData = vehicleDetails.data;
      vehicleDetailData.forEach((item,index) => {
        let tmpObj = {};
        tmpObj["id"] = item.vehicleDetailsId;
        tmpObj["name"] = item.vehicleDetails;
        this.vehicleDetails.push(tmpObj);
      });
      // console.log(vehicleTypeData);
    });

  }


  filterData(){
    this.currentPage = this.activeRoute.snapshot.queryParams.page || 1;
    this.vehicleService.loadFiltereddata(this.filterQueryString, this.currentPage).subscribe((filterData:any) => {
      this.vehiclesList = filterData.data;
      this.totalItems, this.pageOfItems = filterData.data; 
      this.pager = filterData.page;
      // console.log(filterData);
    });
  }

  onChangeSearch(search: string) {
    // fetch remote data from here
    // And reassign the 'data' which is binded to 'data' property.
  }

  onFocused(e) {
    // do something
  }

  open(dialog:any) {
    this.dialogService.open(dialog, { context: 'this is some additional data passed to dialog' });
  }

}