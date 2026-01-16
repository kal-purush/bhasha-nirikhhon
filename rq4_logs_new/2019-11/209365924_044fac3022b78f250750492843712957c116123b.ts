import { Component, OnInit } from '@angular/core';
import { RouterExtensions } from 'nativescript-angular/router';
import { FirebaseService } from '~/app/services/firebase.service';
import { ActivatedRoute } from '@angular/router';
import { ChatComponent } from '../chat/chat.component';


@Component({
  selector: 'ns-group-view',
  templateUrl: './group-view.component.html',
  styleUrls: ['./group-view.component.css']
})
export class GroupViewComponent implements OnInit {

  private groupName;
  private groupID;

  constructor(private router: RouterExtensions, private firebaseService: FirebaseService, private route: ActivatedRoute) { }

  ngOnInit() {
    this.route.queryParams.subscribe(params => {
      this.groupID = params.groupID;
      this.groupName = params.groupName;
  });

  }

}