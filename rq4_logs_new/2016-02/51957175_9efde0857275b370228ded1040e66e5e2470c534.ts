import {Component} from "angular2/core";
import {Member} from "../Member";
import {Validators,FormBuilder,Control,ControlGroup} from "angular2/common";
import {log} from "util";
@Component({
    selector: "new-member",
    inputs: ["member"],
    templateUrl: "app/new-member/new-member.template.html",
    styleUrls: ["app/new-member/new-member.styles.css"],
})

export class NewMemberComponent {
    private newMember: Member;
    newMemberForm: ControlGroup;
    constructor(fb: FormBuilder) {
        this.newMemberForm = fb.group({
            "name": ["", Validators.required],
            "gender": ["", Validators.required]
        });
    }
    addMember() {
        console.log(this.newMemberForm.value);
        this.newMember = {
        "id": 0,
            "name": this.newMemberForm.value.gender + " " + this.newMemberForm.value.name
        };
        console.log(this.newMember);
    }
}