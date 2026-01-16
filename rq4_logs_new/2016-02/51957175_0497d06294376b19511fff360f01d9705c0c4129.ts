import {Component, OnInit, EventEmitter} from "angular2/core";
import {Member} from "../Member";
import {MemberService} from "../member.service";

@Component({
selector: "member-list",
  templateUrl: "app/memberlist/member-list.html",
styleUrls: ["app/memberlist/member-list.css"],
directives: [],
providers: [MemberService],
outputs:['onSelectMember']
})
export class MemberListComponent {
  public members: Member[];
  public selectedMember: Member;
  public onSelectMember = new EventEmitter<Member>();
  constructor (private _MemberService: MemberService){}
  onSelect(member: Member) {
    this.selectedMember = member;
    this.onSelectMember.emit(member);
  }
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