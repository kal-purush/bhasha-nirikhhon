import { Component, OnInit, NgModule } from '@angular/core';
import { AuthService } from '../../../services/auth.service';
import { FormGroup, FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { BrowserModule } from '@angular/platform-browser';
import { TranslatableComponent } from '../../shared/translatable/translatable.component'
import { TranslateService } from '@ngx-translate/core';

@Component({
  selector: 'app-register-manager',
  templateUrl: './register-manager.component.html',
  styleUrls: ['./register-manager.component.css']
})
export class RegisterManagerComponent extends TranslatableComponent implements OnInit {

  registerForm: FormGroup;
  submitted = false;
  roleList = ['Manager'];
  idiomlist = ['en', 'es'];

  constructor(private translateService: TranslateService,private formBuilder: FormBuilder, private authService: AuthService,
    private router: Router) {
   
      super(translateService);
  }


  ngOnInit() {
    this.registerForm = this.formBuilder.group
      ({
        name: [''],
        surname: [''],
        role: [''],
        password: [''],
        email: [''],
        countrycode: [''],
        phone: [''],
        address: [''],
        preferredLanguage:[''],
      });
  }



  onRegister() {


    this.submitted = true;
    if (this.registerForm.valid) {
      console.log ('Hola Mundo');
      console.log (this.registerForm.value);
    this.authService.registerUser(this.registerForm.value)

      .then(res => {

        console.log(res );
     
      }, err => { console.log(err + 'Real error'); });
  }
}
}