import { AlertifyService } from '../_services/alertify.service';
import { AuthAdminService } from '../_services/authAdmin.service';
import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { Router } from '@angular/router';


@Component({
  selector: 'app-navbar',
  templateUrl: './navbar.component.html',
  styleUrls: ['./navbar.component.css']
})
export class NavbarComponent implements OnInit {

  model:any={};
  @Output() featureSelect = new EventEmitter<string>();
  diplayBooking = false;
  displayAdminLgin=false;
  constructor(public authService : AuthAdminService, private alertMessage:AlertifyService, private router:Router) { }

  ngOnInit() {
  }
  logout(){
    this.authService.userToken=null;
    localStorage.removeItem('token');
    this.alertMessage.message('logged out');
    this.router.navigate(['/home']);
  }
  loggedIn(){
      return this.authService.loggedIn();
    
  }
  onSelect(feature:string){
    this.featureSelect.emit(feature);
  }
}