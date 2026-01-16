import { Component, EventEmitter, Output } from "@angular/core";
import { FormControl, Validators } from "@angular/forms";


@Component(
  {
    selector: 'app-login',
    templateUrl: 'login.component.html',
    styleUrls: ['login.component.scss']
  }
)


export class LoginComponent {
  @Output() submit = new EventEmitter<void>()
  hide = true;


  emailFormControl = new FormControl('', [
    Validators.required,
    Validators.email,
  ]);

  passwordFormControl = new FormControl('', [Validators.required]);

  formIsValid(): boolean {
    return this.emailFormControl.valid && this.passwordFormControl.valid;
  }

  formSubmit() {
    this.submit.emit();
  }

}



