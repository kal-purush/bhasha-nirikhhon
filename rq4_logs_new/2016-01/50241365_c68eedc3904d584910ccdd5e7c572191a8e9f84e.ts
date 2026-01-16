import {Component} from 'angular2/core';
import {Allergy} from '../model/allergy';
import {MemberService} from '../services/member.service';
import {OnInit} from 'angular2/core';
import {Member} from "../model/member";


@Component({
    selector: 'member-allergies',
    templateUrl:'app/components/templates/member-allergies.component.html',
    inputs: ['member'],
    providers: [MemberService]
})
export class MemberAllergiesComponent implements OnInit{
    public member: Member;
    public title = 'List of Allergies';
    public allergies:Allergy[];

    constructor(private _memberService: MemberService) { }


    getAllergies() {
        this._memberService.getMemberAllergies()
            .then(allergies => this.allergies = allergies);
    }

    ngOnInit() {
        this.getAllergies();
    }

}