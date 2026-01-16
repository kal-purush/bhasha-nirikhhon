import {Component, EventEmitter, OnInit, Output} from '@angular/core';
import {User} from '../model/user.model';
import {UserService} from '../services/user.service';
import {FormBuilder, FormGroup, Validators} from '@angular/forms';

@Component({
  selector: 'app-admin',
  templateUrl: './service_test.component.html',
  styleUrls: ['./service_test.component.css']
})
export class AdminComponent implements OnInit {
  users: User[];
  form: FormGroup;
  message: string;

  constructor(private userService: UserService,
              private formBuilder: FormBuilder) {
    this.createForm();
  }

  ngOnInit() {
    this.reloadList();
  }

  private createForm(): void {
    this.form = this.formBuilder.group({
        email: ['', Validators.compose([
          Validators.required, Validators.email
        ])],
        password: ['', Validators.required],
        firstName: ['', Validators.required],
        lastName: ['', Validators.required]
      },
    );
  }

  getValidationStyle(controlName: string): any {
    return {
      'is-valid': this.form.get(controlName).dirty && this.form.get(controlName).valid,
      'is-invalid': this.form.get(controlName).dirty && !this.form.get(controlName).valid,
    };
  }

  close(): void {
    this.message = '';
  }

  onSubmit(): void {
    if (this.form.invalid) {
      this.message = 'ERROR - FORM IS INVALID';
      return;
    }
    this.registerUser();
  }

  reloadList(): void {
    this.userService.getAllUsers().subscribe(users => this.users = users);
  }

  private registerUser(): void {
    const pendingUser = this.form.value;
    this.userService.addUser(pendingUser);
  }

  private deleteUser(id: number): void {
    // console.log(id);
    this.userService.deleteUserById(id);
    this.reloadList();
  }

}