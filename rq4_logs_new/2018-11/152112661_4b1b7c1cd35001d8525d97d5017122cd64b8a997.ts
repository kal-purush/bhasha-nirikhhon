import {Component, OnInit, Input} from '@angular/core';
import {first} from 'rxjs/operators';
import {CarHasCompanyModel, CompanyModel, CoownerModel, TokenModel} from '../../../app.component';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {AuthService} from '../../../auth.service';

@Component({
  selector: 'app-car-add-company',
  templateUrl: './car-add-company.component.html',
  styleUrls: ['./car-add-company.component.css']
})
export class CarAddCompanyComponent implements OnInit {
  @Input()
  id: number;
  addForm: FormGroup;
  loading = false;
  submitted = false;
  companies: CompanyModel[] = [];
  companyId = 0;
  selectError = false;

  constructor(
    private connection: AuthService,
    private formBuilder: FormBuilder
  ) { }

  ngOnInit() {
    const token: TokenModel = {
      accessToken: JSON.parse(localStorage.getItem('currentUser'))
    };

    this.connection.getClientsCompanies(token)
       .pipe(first())
       .subscribe(
           data => {
             this.companies = data.companies;
               console.log(data.companies);
           },
           error => {
              console.log(error);
           }
        );
    console.log(this.companies);

      this.addForm = this.formBuilder.group({
          companyName: [],
      });
  }

  get f() { return this.addForm.controls; }

  onSubmit() {
      this.submitted = true;

      for ( const company of this.companies) {
          if (company.name === this.f.companyName.value) {
              this.companyId = company.id;
          }
      }

      if (this.companyId !== 0) {
          this.loading = true;

          const company: CarHasCompanyModel = {
              accessToken: JSON.parse(localStorage.getItem('currentUser')),
              carId: this.id,
              companyId: this.companyId
          };

          this.connection.addCarToCompany(company)
              .pipe(first())
              .subscribe( user => {
                      this.loading = false;
                      window.location.reload();
                  },
                  error => {
                      console.log(error);
                      this.loading = false;
                  });

      } else {
          this.selectError = true;
      }

  }



}