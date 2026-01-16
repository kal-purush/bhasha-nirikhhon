import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { FormGroup, FormControl } from '@angular/forms';
import User from '../../../../shared/model/user.model';

@Component({
  selector: 'app-user-profile-page',
  template: `
  <h2> Profile </h2>
  <div class="col-md-6 col-lg-6 well">
    <form [formGroup]="userGroup">
            <div class='form-group'>
                <label for="firstName">First Name</label>
                <input 
                  type="text" class="form-control" id="firstName" 
                  formControlName="firstName">
            </div>
            <div class='form-group'>
                <label for="lastName">Last Name</label>
                <input 
                  type="text" class="form-control" id="lastName" 
                  formControlName="lastName">
            </div>
            <div class='form-group'>
                <label for="phone">Phone</label>
                <input 
                  type="tel" class="form-control" id="phone" 
                  formControlName="phone">
            </div>
            <div class='form-group'>
                <label for="organization">Organisation</label>
                <input 
                  type="text" class="form-control" id="organization" 
                  formControlName="organization">
            </div>
            <div class='form-group'>
              <label for="region">Region</label>
              <input 
                type="text" class="form-control" id="region" 
                formControlName="region">
          </div>
    </form>
    <button class="btn btn-primary" (click)="loadUser.emit()">Load User</button>
</div>`,
  styleUrls: ['user-profile-page.component.scss']
})
export class UserProfilePageComponent implements OnInit {

  @Input() userGroup: FormGroup;
  @Output() loadUser = new EventEmitter<void>();

  ngOnInit(): void {
  }

}