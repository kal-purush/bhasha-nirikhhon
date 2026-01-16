
    import { Injectable, Output, EventEmitter } from '@angular/core'
    import {MatDialog} from '@angular/material';
    import { UpdateProjectComponent } from '../update-project/update-project.component';
    @Injectable()
    export class UpdateProjectService {
    constructor(public dialog: MatDialog){

    }
 
    
        openDialog(id): void {
            const dialogRef = this.dialog.open(UpdateProjectComponent, {
           
            });
            dialogRef.afterClosed().subscribe(result => {
                console.log('The dialog was closed');
          
              });
            }
    
    }