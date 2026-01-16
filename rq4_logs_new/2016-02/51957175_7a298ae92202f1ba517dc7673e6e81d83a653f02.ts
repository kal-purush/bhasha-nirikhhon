import {Component} from "angular2/core";
import {Member} from "../Member";
import {MemberDetailsComponent} from "../member-details/member-details.component";
import {MemberListComponent} from "../member-list/member-list.component";

@Component({
    selector: "list-view",
    template: `
    <member-list (onSelectMember)="selectedMember = $event"></member-list>
    <member-details [member]="selectedMember"></member-details>
  `,
    directives: [
        MemberDetailsComponent,
        MemberListComponent
    ],
    providers: []
})
export class ListenViewComponent {
    public members: Member[];
    constructor() { }
}