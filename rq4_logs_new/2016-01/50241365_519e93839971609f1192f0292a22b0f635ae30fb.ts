import {Component, Attribute, Input} from 'angular2/core';
import {Allergy} from '../model/allergy';
import {MemberService} from '../services/member.service';
import {OnInit, OnChanges} from 'angular2/core';
import {Member} from "../model/member";


@Component({
    selector: 'member-demographics',
    //templateUrl:'app/components/templates/member-allergies.component.html',
    template: `
      <div *ngIf="member">
        <h2>{{title}}</h2>
        <div><label>id: </label>{{member.externalMemberId}}</div>
        <div><label>First name: </label>{{member.firstName}}</div>
        <div><label>Last name: </label>{{member.lastName}}</div>
        <div><label>Age: </label>{{member.age}}</div>
        <div><label>Gender: </label>{{member.gender.value}}</div>
        <div><label>Date of birth: </label>{{member.birthDate}}</div>
     </div>
    `,
    inputs: ['member'],
    providers: [MemberService]
})
export class MemberDemographicsComponent implements OnChanges {
    public member:Member;
    public title = "Demographics";
    public allergies:Allergy[];

    constructor(private _memberService:MemberService) {
    }


    ngOnChanges(){
    }
}