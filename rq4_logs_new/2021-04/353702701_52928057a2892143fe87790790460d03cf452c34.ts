import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { ToastrService } from 'ngx-toastr';
import { clientType } from 'src/app/shared/enums/clientType';
import { ClientService } from '../client.service';

@Component({
  selector: 'app-add-client',
  templateUrl: './add-client.component.html',
  styleUrls: ['./add-client.component.scss']
})
export class AddClientComponent implements OnInit {

  clientForm = new FormGroup({});
  
  // Typ klienta
  typeOfClientDefault = clientType.normalny ;
  typeOfClient = Object.values(clientType);

  // Walidacja formularza
  emailRegex = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/;
  passwordRegex = /^(?=.*\d)(?=.*[a-z])(?=.*[A-Z])(?=.*[a-zA-Z]).{8,}$/;
  phoneRegex = /^[+,\d]\d{7,12}$/;


  constructor(private cd: ChangeDetectorRef,    
              private clientService: ClientService,
              private formBuilder: FormBuilder,
              private router: Router,
              private toastr: ToastrService,
              public dialog: MatDialog) { }

  ngOnInit(): void {
    this.createFormClient();
  }

fieldTextType?: boolean;

toggleFieldTextType() {
  this.fieldTextType = !this.fieldTextType;
}

  createFormClient() : void {
    this.clientForm = this.formBuilder.group({
      name: ['', Validators.compose([Validators.required, Validators.minLength(1), Validators.maxLength(15)])],
      surname: ['', Validators.compose([Validators.required, Validators.minLength(1), Validators.maxLength(15)])],
      phone: ['',Validators.compose([Validators.required, Validators.pattern(this.phoneRegex)])],
      email: ['',Validators.compose([Validators.required, Validators.pattern(this.emailRegex)])],
      city:['', Validators.compose([Validators.required, Validators.minLength(1), Validators.maxLength(40)])],
      street: ['', Validators.compose([Validators.required, Validators.minLength(1), Validators.maxLength(40)])],
      type: [this.typeOfClientDefault,Validators.required],
      peselOrNIP: ['',Validators.compose([Validators.required, Validators.minLength(10), Validators.maxLength(11)])],
      documentID: ['', Validators.compose([Validators.required, Validators.minLength(6), Validators.maxLength(10)])],
      other: ['', Validators.compose([Validators.required, Validators.minLength(1), Validators.maxLength(250)])]      
  });
    this.cd.markForCheck();
  }

  addClient() : void {
    this.clientService.addClient(this.clientForm.value).subscribe(() => {
      console.log('formularz' + this.clientForm.value);
      this.router.navigate(['/clients']);
      this.showToasterAddClient();
    }, err => {   
        this.showToasterAddClientError(err.error.message);
      }
    );
  }

  reset() : void {
    this.clientForm.reset({
        type: this.typeOfClientDefault
    });
  }

  showToasterAddClient() : void {
    this.toastr.success(this.clientForm.controls.name.value + " " + this.clientForm.controls.surname.value + " został dodany pomyślnie!");
  }

  showToasterAddClientError(err : string) : void {
    let lenghtErr = err.length;
    let text = err.substr(0,lenghtErr-1);
    this.toastr.error(text);
  }
}