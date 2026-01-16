import {Component} from 'angular2/core';
import {Member} from "../model/member";
import {MemberService} from "../services/member.service";
import {OnInit} from 'angular2/core';
import {MemberDetailComponent} from "./member-detail.component";
import {MemberTilesComponent} from "./member-tiles.component";


@Component({
    selector: 'member-list',
    templateUrl:'app/components/templates/member-list.component.html',
    styleUrls:['app/components/css/member-list.component.css'],
    directives: [MemberDetailComponent, MemberTilesComponent]
})
export class MemberListComponent implements OnInit{
    public title = 'List of Members';
    public selectedMember:Member;
    public members:Member[];

    constructor(private _memberService: MemberService) { }

    onSelect(member: Member) {
        this.selectedMember = member;
        console.log("Member:" + this.selectedMember.firstName + " " + this.selectedMember.lastName);
    }

    getMembers() {
        this._memberService.getMembers().then(members => this.members = members);
    }

    ngOnInit() {
        this.getMembers();
    }

}