
import { Component, OnInit } from '@angular/core';
import { ShelterService } from '@appCore/services/shelter.service';
import { ActivatedRoute } from '@angular/router';
import { filter } from 'rxjs/operators';


@Component({
    selector: 'app-shelters-signup',
    templateUrl: 'shelters-signup.component.html',
    styleUrls: ['./shelters-signup.component.css']
})

export class SheltersSignupComponent implements OnInit {

    constructor(
        private _activated: ActivatedRoute,
        private _shelterService: ShelterService
    ) { }

    ngOnInit() {
      console.log('YO');
    }


}