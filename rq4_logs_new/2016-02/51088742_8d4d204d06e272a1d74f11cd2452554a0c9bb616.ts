import {Component} from 'angular2/core';
import {NgForm}    from 'angular2/common';
import {HotelImpl} from "./hotelImpl";
import {OnInit} from "angular2/core";
import {HTTP_PROVIDERS} from "angular2/http";
import {HotelService} from "./hotelService";
import {HotelInterface} from "./hotelInterface";

@Component({
    selector: 'hotel-form',
    templateUrl: 'html/hotel-form.component.html',
    providers:[HotelService,HTTP_PROVIDERS],
})
export class HotelFormComponent{
    model = new HotelImpl('','');
    public hotels : HotelInterface[];
    submitted = false;
    onSubmit() {
        this.submitted = true;
    }

    active = true;
    newHotel() {
        this.model = new HotelImpl('','');
        this.active = false;
        setTimeout(()=> this.active=true, 0);
    }
    // TODO: Remove this when we're done
    get diagnostic() { return JSON.stringify(this.hotels); }

    constructor(private _hotelService: HotelService) { };
    searchHotel(arr) {
        this._hotelService.getHotels(arr)
            .subscribe(
                f => this.hotels = f,
                error => alert(`Server error. Try again later`));
    }


}