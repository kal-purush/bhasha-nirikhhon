import { Component, OnInit } from '@angular/core';
import { RouterExtensions } from 'nativescript-angular/router';
import { FirebaseService } from '~/app/services/firebase.service';
import { ActivatedRoute } from '@angular/router';
const firebase = require("nativescript-plugin-firebase");

@Component({
  selector: 'ns-group-members',
  templateUrl: './group-members.component.html',
  styleUrls: ['./group-members.component.css']
})
export class GroupMembersComponent implements OnInit {

  public groupID;
  public groupName;
  public groupMembers = [];

  constructor(private router: RouterExtensions, private firebaseService: FirebaseService, private route: ActivatedRoute) { }

  ngOnInit() {
    this.route.queryParams.subscribe(params => {
      this.groupID = params.groupID;
      // this.groupName = params.groupName;
      this.getGroupMembers();
    });
  }

  getGroupMembers() {
    const this_ = this;
    firebase.getValue('groups/'+this_.groupID+'/users').then( result => {
      if (result && result.value) {
        for (const member of result.getValue()) {
          if (member.value === true)
            this_.groupMembers.push(member.value);
        }
        // console.log(result);
      }
    })
  }

}