/**
 * Created by mclaveau on 01/02/2016.
 */
import {Component} from 'angular2/core';
import {Flight} from "./flight";
import {Hotel} from "./hotel";
import {Reservation} from "./reservation";
import {FlightService} from "./flightService";
import {OnInit} from "angular2/core";
import {HTTP_PROVIDERS} from "angular2/http";
@Component({
    selector: 'my-app',
    templateUrl:'app/app.html',
    providers:[FlightService,HTTP_PROVIDERS],
})
export class AppComponent{
    public title = 'Formulaire réservation';
    public flights: Flight[];
    constructor(private _flightService: FlightService) { };
    searchFlight(dep,arr,dat) {
        this._flightService.getFlights(dep ,arr,dat)
            .subscribe(
                f => this.flights = f,
                error => alert(`Server error. Try again later`));
    }
    public hotels: Hotel[];
    public reservation : Reservation;


}