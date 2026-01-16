import {Component} from 'angular2/core';
import {Member} from './member';
import {MemberDetailComponent} from './member-detail.component';
import {MemberService} from './member.service';
import {OnInit} from 'angular2/core';

@Component({
    selector: 'trucare-app',
    template: `
              <h2>List of members</h2>
                <ul class="members">
                  <li *ngFor="#member of members" [class.selected]="member === selectedMember" (click)="onSelect(member)">
                      <span class="badge">{{member.id}}</span> {{member.name}}
                  </li>
                </ul>


              <member-detail [member]="selectedMember"></member-detail>
              ` ,
    styles:[`
          .selected {
            background-color: #CFD8DC !important;
            color: white;
          }
          .members {
            margin: 0 0 2em 0;
            list-style-type: none;
            padding: 0;
            width: 10em;
          }
          .members li {
            cursor: pointer;
            position: relative;
            left: 0;
            background-color: #EEE;
            margin: .5em;
            padding: .3em 0em;
            height: 1.6em;
            border-radius: 4px;
          }
          .members li.selected:hover {
            color: white;
          }
          .members li:hover {
            color: #607D8B;
            background-color: #EEE;
            left: .1em;
          }
          .members .text {
            position: relative;
            top: -3px;
          }
          .members .badge {
            display: inline-block;
            font-size: small;
            color: white;
            padding: 0.8em 0.7em 0em 0.7em;
            background-color: #607D8B;
            line-height: 1em;
            position: relative;
            left: -1px;
            top: -4px;
            height: 1.8em;
            margin-right: .8em;
            border-radius: 4px 0px 0px 4px;
          }
        `],
    directives: [MemberDetailComponent],
    providers: [MemberService]
})
export class AppComponent implements OnInit{
    public title = 'List of Members';
    public selectedMember:Member;
    public members:Member[];

    constructor(private _memberService: MemberService) { }

    onSelect(member: Member) { this.selectedMember = member; }
    onSelect(member: Member) { this.selectedMember = member; }

    getMembers() {
        this._memberService.getMembers()
            .then(members => this.members = members);
    }

    ngOnInit() {
        this.getMembers();
    }

}

