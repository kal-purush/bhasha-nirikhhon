import { Component, OnInit } from '@angular/core';
import {IAirplane} from "../WebModels/iairplane";
import {SelectItem} from "primeng/api";
import {HttpClient, HttpHeaders} from "@angular/common/http";
import {WebRestService} from "../WebService/web-rest.service";
import {SynchronizerService} from "../WebService/synchronizer.service";
import {IRunway} from "../WebModels/irunway";
import {IParking} from "../WebModels/iparking";

@Component({
  selector: 'landing-plane-control',
  templateUrl: './landing-plane-control.component.html',
  styleUrls: ['./landing-plane-control.component.css']
})
export class LandingPlaneControl {

  public test:Date = new Date();

  private landingAirplanes: IAirplane[] = [];

  public runways: IRunway[] = [];

  public parkingPositions: IParking[] = [];

  public items: SelectItem[] = [];

  public selectedAirplane: IAirplane;

  public constructor(private http:HttpClient, private service: WebRestService, private sync:SynchronizerService){
    this.sync.Clock.subscribe(() => this.LoadData());
  }

  public LoadData(){
    this.landingAirplanes = this.service.GetAirplanes("Landing")
      .concat(this.service.GetAirplanes("Landed"));

    this.items = this.landingAirplanes.map((airplane:IAirplane) => {
      return {
        label: airplane.identifier.name,
        value: airplane,
        disabled: (airplane.status=='Landed' || airplane.runway != null)};
    });

    this.http.get(this.service.BaseUrl+'runways')
      .subscribe(value =>{
        this.runways = value as IRunway[];
      });

    this.http.get<IParking[]>(this.service.BaseUrl+'parkings')
      .subscribe(value =>{
        this.parkingPositions = value;
      });
  }

  public ReserveRunway(runway:IRunway){
    this.http.get(this.service.BaseUrl+'runway/landing?Airplane='+this.selectedAirplane.number+
      '&Runway='+runway.id).subscribe(value => {
        if(value){
          alert("Runway reserved!");
        }else{
          alert("Runway could not be reserved!");
        }
    });
  }

  public UpdateRAT(airplane:IAirplane){
    airplane.realArrivalTime = Date.now();
    //headers.append("content-type","application/json");
    this.http.get(this.service.BaseUrl+'airplane/update?Airplane='+airplane.number+'&RAT='+airplane.realArrivalTime)
      .subscribe(value => {
        alert("Updated Real Arrival Time of "+airplane.identifier.name);
      });
  }

  public GetAirplane(runway: IRunway): IAirplane{
    return this.landingAirplanes.find((airplane:IAirplane):boolean => {
      if(airplane.runway != null){
        return airplane.runway.id == runway.id;
      }
      return false;
    });
  }

  public GetDateTimeString(dateValue:number):string {
    return this.service.GetDateTimeString(dateValue);
  }
}