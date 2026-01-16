import { Component, OnInit } from '@angular/core';
import { UserService } from '../../../services/user.service';
import { MdDialogRef } from '@angular/material';

@Component({
  selector: 'app-details-modal',
  templateUrl: './details-modal.component.html',
  styleUrls: ['./details-modal.component.css']
})
export class DetailsModalComponent implements OnInit {

  constructor(public dialogRef: MdDialogRef<DetailsModalComponent>,
              public userService: UserService) { 
      this.userService.doSignIn$.subscribe(()=>{
        this.dialogRef.close();
      })
  }

  ngOnInit() {
  }

}