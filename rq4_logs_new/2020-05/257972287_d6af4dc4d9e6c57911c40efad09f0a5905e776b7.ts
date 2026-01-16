import { Component, OnInit, OnDestroy } from '@angular/core';
import { AuthService } from '../services/auth.service';
import { Subscription } from 'rxjs';
import { ActivatedRoute, Router } from '@angular/router';
import { UserService } from '../services/user.service';

@Component({
  selector: 'app-user-info',
  templateUrl: './user-info.component.html'
})
export class UserInfoComponent implements OnInit, OnDestroy {

  me: any;
  subs: Subscription;
  subs2: Subscription;
  subs3: Subscription;
  userId: string;
  myPropertyId: string;
  userInfo: any;
  change = true;
  following = false;
  
  constructor(private authService: AuthService,
              private userService: UserService,
              public _route: ActivatedRoute,
              public router: Router) { }



  ngOnInit() {
    this._route.params.subscribe(params => {
      this.userId = String(params['id']);
    });

    this.subs = this.authService.userStatusChanges
    .subscribe( userDetails => {
      this.me = userDetails;
    });

    this.subs2 = this.userService.getUserByOtherId(this.userId)
    .subscribe( user => {
      this.userInfo = user;
    });

    this.subs3 = this.userService.getUserByOtherId(this.me.id)
    .subscribe( user => {
      this.myPropertyId = user.propertyId;
    });

    console.log('Ignoren el fallo que sale en consola');
  }

  ngOnDestroy(): void {
    this.subs.unsubscribe();
    this.subs2.unsubscribe();
    this.subs3.unsubscribe();
  }

  viewPosts(){
    this.change = !this.change
  }

  follow(){
    this.following = true;
  }
  
  unfollow(){
    this.following = false;
  }


}