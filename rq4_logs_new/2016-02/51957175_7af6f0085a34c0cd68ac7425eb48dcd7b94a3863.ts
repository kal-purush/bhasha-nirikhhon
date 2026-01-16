import {Component, OnInit} from "angular2/core";
import {Member} from "./Member";
import {MemberDetailsComponent} from "./member-details/member-details.component";
import {MemberService} from "./member.service";


@Component({
selector: "my-app",
  template: `
    <h1>{{title}}</h1>
    <h2>My Heroes</h2>
    <ul class="member">
      <li *ngFor="#member of members"
        [class.selected]="member === selectedMember"
        (click)="onSelect(member)">
        <span class="badge">{{member.id}}</span> {{member.name}}
      </li>
    </ul>
    <member-details [member]="selectedMember"></member-details>
  `,
styles: [`
.selected {
background-color: #CFD8DC !important;
color: white;
}
.member {
margin: 0 0 2em 0;
list-style-type: none;
padding: 0;
width: em;
}
.member li {
cursor: pointer;
position: relative;
left: 0;
background-color: #EEE;
margin: .5em;
padding: .3em 0em;
height: 6em;
border-radius: 4px;
}
.member li.selected:hover {
color: white;
}
.member li:hover {
color: #607D8B;
background-color: #EEE;
left: .1em;
}
.member .text {
position: relative;
top: -3px;
}
.member .badge {
display: inline-block;
font-size: small;
color: white;
padding: 0.8em 0.7em 0em 0.7em;
background-color: #607D8B;
line-height: 1em;
position: relative;
left: -1px;
top: -4px;
height: 8em;
margin-right: .8em;
border-radius: 4px 0px 0px 4px;
}
`],

directives: [MemberDetailsComponent],
providers: [MemberService]
})
export class AppComponent {
public title = "Das Team";
public members: Member[];
public selectedMember: Member;
constructor (private _MemberService: MemberService){}
onSelect(member: Member) { this.selectedMember = member; }
  getMembers() {
    this._MemberService.getMembers()
        .subscribe(
            data => this.members = data,
            error => console.log(error),
            () => console.log("Finished")
            
         );
  }
  ngOnInit() {
    this.getMembers();
  }
}

// let MEMBERS: Member[] = [
// { "id": 11, "name": "Mr. White" },
// { "id": 12, "name": "Mr. Orange" },
// { "id": 13, "name": "Mr. Blonde" },
// { "id": 14, "name": "Mr. Pink" },
// { "id": 15, "name": "Mr. Brown" },
// { "id": 16, "name": "Mr. Blue" },
// { "id": 17, "name": "Mr. Black" },
// { "id": 18, "name": "Mr. Gray" },
// { "id": 19, "name": "Mr. Yellow" },
// { "id": 20, "name": "Mr. Green" }
// ];