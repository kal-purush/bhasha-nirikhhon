import { Component, OnInit } from '@angular/core';
import { MatDialogRef } from '@angular/material';
import { User } from '../../models/user';
import { FormControl, Validators } from '@angular/forms';
import { UserService } from '../../services/user.service';

@Component({
  selector: 'app-new-user-dialog',
  templateUrl: './new-user-dialog.component.html',
  styleUrls: ['./new-user-dialog.component.scss']
})
export class NewUserDialogComponent implements OnInit {

  avatars = [
    'svg-1', 'svg-2', 'svg-3', 'svg-4'
  ];
  user: User;
  skills = [''];

  constructor(
    private dialogRef: MatDialogRef<NewUserDialogComponent>,
    private userService: UserService) { }

  name = new FormControl('', [Validators.required]);

  getErrorMessage() {
    return this.name.hasError('required') ? 'You must enter a name' : '';
  }

  ngOnInit() {
    this.user = new User();
  }

  addNewSkill() {
    this.skills[this.skills.length] = '';
  }

  removeSkill(index: number) {
    if (this.skills.length > 1) {
      for (let i = index; i < this.skills.length; i++) {
        this.skills[i] = this.skills[i + 1];
      }
    this.skills.length = this.skills.length - 1;
    }
  }

  addNewUser() {
    console.log(this.name);
    console.log(this.skills);

    const user: any = {
      id: -1 , // this will be set in the service
      numClaps: 0,
      name: this.name,
      skills: this.skills
    };

    console.log(user);
    this.userService.addUser(user);
  }

  customTrackBy(index: number, obj: any): any {
    return index;
  }

  save() {
    console.log(this.user);
    this.userService.addUser(this.user).then(user => {
      this.dialogRef.close(user);
    });
  }

  dismiss() {
    this.dialogRef.close(null);
  }

}