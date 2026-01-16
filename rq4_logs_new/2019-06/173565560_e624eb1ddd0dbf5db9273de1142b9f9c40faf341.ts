import { Injectable } from '@angular/core';
import { MatDialogConfig, MatDialog } from '@angular/material';
import { RegisterComponent } from './components/header/register/register.component';
import { LoginComponent } from './components/header/login/login.component';

@Injectable({
  providedIn: 'root'
})
export class PopUpService {
  

  constructor(private dialog:MatDialog,
    private dialog2:MatDialog ) { }

  openDialog(){
    const dialogConfig = new MatDialogConfig();
   
    dialogConfig.width = "300%";
    //dialogConfig.height = "900px";
    this.dialog.open(RegisterComponent)
  }
  openDialog2(){
    const dialogConfig2 = new MatDialogConfig();
    
    this.dialog2.open(LoginComponent)
  }
  closeDialog(){
   this.dialog.closeAll();
  }
  closeDialog2(){
    this.dialog2.closeAll();
   }
}