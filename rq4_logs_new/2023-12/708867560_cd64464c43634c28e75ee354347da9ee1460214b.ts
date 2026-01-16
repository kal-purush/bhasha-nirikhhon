import {Component, OnInit} from '@angular/core';
import {AuthService} from "../../shared/services/auth.service";
import {User} from "../../shared/services/user";
import {navBarDate} from "../dashboard/navbarData";
import {UsersService} from "../../shared/services/users.service";
import {Observable, switchMap} from "rxjs";
import {userData} from "../users-table/users-table.component";
import {ActivatedRoute,} from "@angular/router";

@Component({
  selector: 'app-user-info',
  templateUrl: 'user-info.component.html',
  styleUrls: ['./user-info.component.css'],
  providers: [UsersService]
})
export class UserInfoComponent implements OnInit {

  public readonly navBarDate = navBarDate;
  userProfile$: Observable<userData>  ;
  constructor
  (private userService: UsersService,
   private authService: AuthService,
   private route: ActivatedRoute)
  {

  }
  ngOnInit() {
     this.userProfile$= this.route.params
       .pipe(
         switchMap(params => this.userService.showUserProfile(+params.id))
       )
  }
  get user(): User {
    return  this.authService.userData
  }
  signOut() {
    this.authService.signOut().then(r => r);
  }
}