import {Component, OnInit} from '@angular/core';
import {CompanyModel} from '../app.component';
import {AuthService} from '../auth.service';
import {Router} from '@angular/router';

@Component({
  selector: 'app-company-list',
  templateUrl: './company-list.component.html',
  styleUrls: ['./company-list.component.css']
})
export class CompanyListComponent implements OnInit {

  private companies: CompanyModel[] = [];

  constructor(private authService: AuthService,
              private router: Router) {
  }

  ngOnInit() {
    this.authService.getAllCompanies().subscribe(
      (result) => {
        this.authService.setExpirationDate();
        this.companies = result.companies;
        console.log(this.companies);
      }
    );
  }

  edit(i) {
    this.router.navigate(['employee/account/editCompany/', this.companies[i].id]);
  }

  addCompany() {
    this.router.navigate(['employee/account/addCompany']);
  }
}