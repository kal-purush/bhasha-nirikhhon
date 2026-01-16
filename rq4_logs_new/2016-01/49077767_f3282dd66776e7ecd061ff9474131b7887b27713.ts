import {Component} from 'angular2/core';
import {MeetupService} from './meetup-service';
import {Meetup} from "./meetup-model";

@Component({
    selector: 'meetup-search',
    templateUrl: 'app/components/meetups/search.html',
    providers: [MeetupService]
})
export class MeetupSearchComponent {

    constructor(private meetupService:MeetupService) {}

    result:Meetup[];
    city:string;
    countryCode:string;


    onSubmit() {
        this.meetupService.getList(this.city, this.countryCode)
            .subscribe(
                data => this.result = JSON.parse(data.text()).map((o) => Meetup.fromObject(o)),
                err => {
                    console.log(err);
                    this.result = null;
                },
                () => console.log('Meetup results loaded')
            );
    }
}