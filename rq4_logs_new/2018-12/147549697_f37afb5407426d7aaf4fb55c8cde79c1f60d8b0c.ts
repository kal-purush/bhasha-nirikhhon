import { Component, OnInit, Input, HostBinding } from '@angular/core';
import {MatDialog} from '@angular/material';
import {TodoDetailComponent} from '../../todo-detail/todo-detail.component'
@Component({
  selector: 'app-detail-todo-button',
  templateUrl: './update-button.component.html',
  styleUrls: ['./update-button.component.scss'],
})

export class UpdateButtonTodoComponent implements OnInit{
  public id
  public projId
  @Input() parentData
  
  constructor(public dialog: MatDialog) { 
    
  }
  
  
  openDialog(id): void {
    this.id = id
    console.log(this.id)
    const dialogRef = this.dialog.open(TodoDetailComponent, {
      data: {
        id: this.id,
      }
    });
    dialogRef.afterClosed().subscribe(result => {
      console.log('The dialog was closed');
      
    });
    
    
  }
  ngOnInit() {
    
  }
  
  
}