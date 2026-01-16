import { Component, OnInit, OnDestroy } from '@angular/core';
import { FormBuilder, FormGroup, FormArray, AbstractControl, Validators, FormGroupDirective } from '@angular/forms';
import { UserService } from '@dashboard/services/user.service';
import { Subscription } from 'rxjs';
import { Router, ActivatedRoute } from '@angular/router';
import { IUser } from '@interfaces/user';
import { IRole } from '@interfaces/embedded.documents';

@Component({
  selector: 'app-user-form',
  templateUrl: './user-form.component.html',
  styleUrls: ['./user-form.component.sass']
})
export class UserFormComponent implements OnInit, OnDestroy {

  private subscriptions: Subscription = new Subscription();
  userForm: FormGroup;
  isEdit = false;
  hide = true;
  roles: IRole[] = [
    { value: 'employee', viewValue: 'Empleado' },
    { value: 'objective', viewValue: 'Objetivo' },
    { value: 'coordinator', viewValue: 'Coordinador' },
    { value: 'admin', viewValue: 'Administrador' },
    { value: 'dev', viewValue: 'Desarrollador' }
  ];

  constructor(
    private fBuilder: FormBuilder,
    private userService: UserService,
    private router: Router,
    private activatedRoute: ActivatedRoute
  ) {}

  ngOnInit(): void {
    this.initUserForm();

    // get param id on edit
    const { id } = this.activatedRoute.snapshot.params;
    if (id) {
      this.subscriptions.add(
        this.userService.getUser(id).subscribe(
          user => {
            this.isEdit = true;
            this.editUser(user);
        }));
    }
  }

  ngOnDestroy() {
    this.subscriptions.unsubscribe();
  }

  // init user form
  initUserForm() {
    this.userForm = this.fBuilder.group({
      _id: [''],
      username: ['', Validators.required],
      password: ['', Validators.required],
      email: ['', Validators.required],
      role: ['', Validators.required],
      profile: this.fBuilder.group({
        firstName: ['', Validators.required],
        lastName: ['', Validators.required],
        avatar: ['https://i0.wp.com/www.mvhsoracle.com/wp-content/uploads/2018/08/default-avatar.jpg?ssl=1'],
        dni: ['', Validators.required]
      }),
    });
  }

  // set user DB values on the form
  editUser(user: IUser) {
    this.userForm.patchValue({
      _id: user._id,
      username: user.username,
      email: user.email,
      role: user.role,
      profile: user.profile
    });
    const profile: FormGroup = this.userForm.get('profile') as FormGroup;
  }

  // Create user
  saveClickEvent(userNgForm: FormGroupDirective): void {
    if (this.userForm.valid) {
      this.subscriptions.add(
        this.userService.addUser(this.userForm.value).subscribe(
          success => {
            if (success) {
              this.router.navigate(['/dashboard/usuarios']);
            }
          }
      ));
    } else {
      console.log("entra");
    }
  }

  // update user
  updateClickEvent(userNgForm: FormGroupDirective): void {
    if (this.userForm.valid) {
      this.subscriptions.add(
        this.userService.updateUser(this.userForm.value).subscribe(
          success => {
            if (success) {
              this.router.navigate(['/dashboard/usuarios']);
            }
          }
      ));
    }
  }


  get username(): AbstractControl {
    return this.userForm.get('username');
  }

  get role(): AbstractControl {
    return this.userForm.get('role');
  }

  get password(): AbstractControl {
    return this.userForm.get('password');
  }

  get email(): AbstractControl {
    return this.userForm.get('email');
  }

  get firstName(): AbstractControl {
    return this.userForm.get('profile').get('firstName');
  }

  get lastName(): AbstractControl {
    return this.userForm.get('profile').get('lastName');
  }

  get avatar(): AbstractControl {
    return this.userForm.get('profile').get('avatar');
  }

  get dni(): AbstractControl {
    return this.userForm.get('profile').get('dni');
  }

}