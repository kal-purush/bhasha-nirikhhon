import {Component, OnInit} from 'angular2/core';
import {Family} from './family.interface';
import {FamilyService} from './family.service';
import {CORE_DIRECTIVES} from 'angular2/common';
import {Configuration} from './configuration';

@Component({
    selector: 'family-list',
    templateUrl: 'app/family-list.html',
    providers: [FamilyService, Configuration],
    directives: [CORE_DIRECTIVES]
})

export class FamilyList implements OnInit {
    //families = ['fam1', 'fam2', 'fam3'];

    constructor(private _famService: FamilyService) {}
    errorMessage: string;
    families: Family[];

    ngOnInit() { this.getFamilies(); }

    getFamilies() {
        this._famService.getFamilies()
            .subscribe(
            families => this.families = families,
            error => this.errorMessage = <any>error);
    }
}