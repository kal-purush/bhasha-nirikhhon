import { Component, OnInit } from '@angular/core';
import { UserService } from 'src/app/services/user.service';
import { IUser } from 'src/app/interfaces/user';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import Swal from 'sweetalert2';
import { AuthService } from 'src/app/services/auth.service';

@Component({
  selector: 'app-list-user',
  templateUrl: './list-user.component.html',
  styleUrls: ['./list-user.component.css']
})
export class ListUserComponent implements OnInit {
  users:any;
  formRole:FormGroup;
  userId:string;
  auth:any;
  constructor(private userService:UserService, private formBuilder:FormBuilder, private authService:AuthService) { 
    this.formRole=this.formBuilder.group({
      userRole:['',[Validators.required]]

    })
  }

  ngOnInit(): void {
    this.userService.getUsersData().subscribe(res=>{
      this.users=res;
      console.log(res);
      
    });
    this.auth=this.authService.getUserID();
    console.log(this.auth);
    

  }
  changeRole(){
    if(this.auth==this.userId){
      Swal.fire({
        icon:'error',
        text:'no tiene permitido realizar esta acción'
      });
    }else{
      this.userService.changeRole(this.formRole.value,this.userId ).subscribe(
        res=>{
        console.log(res);
        document.getElementById('changeRole').click();
        Swal.fire({
          icon:'success',
          text:'Se ha modificado el tipo de usuario exitosamente'
        });
      },
      err=>{
        document.getElementById('changeRole').click();
        Swal.fire({
          icon:'error',
          text:'no se ha podido realizar la solicitud'
        });
      })
    }
    

  }
  cargarDatos(user:any){
    var name=document.getElementById('name').setAttribute('value',user.name);
    var name=document.getElementById('email').setAttribute('value',user.email);
    this.formRole.patchValue({userRole:user.userRole});
    this.userId=user._id;
    console.log(this.userId);
    

  }

}