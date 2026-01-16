import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { CustomerService } from '../services/customer.service';

@Component({
  selector: 'app-profile',
  templateUrl: './profile.component.html',
  styleUrls: ['./profile.component.css']
})
export class ProfileComponent implements OnInit {

  constructor(
    private service : CustomerService,
    private route: Router
  ) {}

  ngOnInit(): void {
    if (localStorage.getItem('customer') == null) {
      this.route.navigate(['login']);
    }
  }

  foreclouser() {
    this.service.requestForeclouser();
  }

  showPersonalDetails(){
    this.route.navigate(['profile/customerlist']);
  }

  showLoanDetails(){
    this.route.navigate(['profile/loanlist']);
  }

  showCapitalDetails(){
    this.route.navigate(['profile/capitallist']);
  }

  showAuthDetails(){
    this.route.navigate(['profile/authdoclist']);
  }
}