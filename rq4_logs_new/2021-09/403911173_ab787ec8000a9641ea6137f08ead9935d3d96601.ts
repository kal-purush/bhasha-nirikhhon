import {Component, OnInit} from '@angular/core';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';
import {LoginMockerService} from './login.mocker.service';
import {Router} from '@angular/router';

@Component({
  templateUrl: 'access.page.html'
})
export class AccessPage implements OnInit{
  loginForm: FormGroup;
  loading = false;
  submitted = false;
  hide = true;

  user: any;

  constructor(private formBuilder: FormBuilder, private service: LoginMockerService, private router: Router) {
  }

  ngOnInit(): void {
    this.loginForm = this.formBuilder.group({
      username: ['', [Validators.required]],
      password: ['', Validators.required]
    });
  }

  onSubmit(): void {
    this.submitted = true;

    // stop here if form is invalid
    if (this.loginForm.invalid) {
      return;
    }

    this.loading = true;

    if (this.isValid()) {
      this.service.login(this.user).subscribe(res => {
        LoginMockerService.isLog = true;
        this.router.navigate(['/home']);
      });
    }
  }

  getObjectFromView(): void{
    this.user.login = this.loginForm.get('username').value;
    this.user.password = this.loginForm.get('password').value;
  }

  public isValid(): boolean {
    this.getObjectFromView();
    return this.loginForm.valid;
  }
}