import { Component, OnInit, Input, HostBinding } from '@angular/core';
import {MatDialog} from '@angular/material';
import {UpdateProjectComponent} from '../../update-project/update-project.component'
@Component({
  selector: 'app-update-button',
  templateUrl: './update-button.component.html',
  styleUrls: ['./update-button.component.scss'],
})

export class UpdateButtonComponent implements OnInit{
public id
@Input() parentData

  constructor(public dialog: MatDialog) { 

  }


  openDialog(id): void {
    this.id = id
    console.log(this.id)
    const dialogRef = this.dialog.open(UpdateProjectComponent, {
      data: {
        id: this.id
      }
    });
    dialogRef.afterClosed().subscribe(result => {
        console.log('The dialog was closed');
  
      });


    }
    ngOnInit() {
    
    }


}