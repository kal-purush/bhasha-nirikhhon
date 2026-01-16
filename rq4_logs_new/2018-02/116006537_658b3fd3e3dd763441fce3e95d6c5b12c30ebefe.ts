/*
Ver el siguiente modulo para implementar un multiselect dropdown
https://github.com/CuppaLabs/angular2-multiselect-dropdown


Varios diseños 
https://github.com/mdbootstrap/Angular-Bootstrap-with-Material-Design
*/
import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';


import { LoginService } from './services/login.service';
import { UsersService } from './services/users.service';
import { ClassroomsService } from './services/classrooms.service';

import {User} from './classes/User.class';
import { Classroom } from './classes/Classroom.class';
import { Response } from '@angular/http/src/static_response';
import { error } from 'selenium-webdriver';
import { toBase64String } from '@angular/compiler/src/output/source_map';

@Component({
  selector: 'classrooms-component',
  templateUrl: './templates/classrooms.template.html',
  styleUrls: ['./templates/css/sidemenu.css','./templates/css/perfil.css',  './templates/font-awesome/css/font-awesome.css']
})

export class ClassroomsComponent implements OnInit {
    opcion: string = 'crear';

    //Crear Evento
    message_to_show = ""
    classrooms: Classroom[];
    classroom: string = '';
    level: string = '';

    //MultiSelect Dropdown variables
    itemList = []
    selectedItems = [];
    settings = {};

    // test
    private fileReader: FileReader;
    private base64Encoded: string;

    constructor(private ServicioLogin: LoginService,private ServicioClassroom: ClassroomsService ,private ServicioUsers :UsersService, private Ruta: ActivatedRoute, private router: Router) {
    }

    ngOnInit() {
          if(!this.ServicioLogin.isLogged) {
            this.router.navigateByUrl('/');
          }else{
            this.itemList = [      
              {"id":1,"itemName":"Infantil"},
              {"id":2,"itemName":"Primaria"},
              {"id":3,"itemName":"ESO"}
            ]
            this.selectedItems = [];
            this.settings = {
              singleSelection: true,
              text: "Seleecciona el nivel",
              selectAllText: 'Select All',
              unSelectAllText: 'UnSelect All',
              classes: "myclass custom-class"
          };
            // Conseguimos las clases
            this.ServicioClassroom.getClassrooms().subscribe(
              response => {
                this.classrooms = response
              },
              error => console.log(error)
              
            )
          }      
        }

    //-----MultiSelect Dropdown ---
    createClassroom() {
      //var pieces = this.date.split('-')
      console.log(this.selectedItems)
      this.ServicioClassroom.create_classroom(this.classroom, this.selectedItems[0]['itemName']).subscribe(
        response => {
          this.message_to_show = "Created correctly"
          this.classroom = ""
          this.level = ""
          console.log(response)
        },
        error=>{
          console.log(error)
          var status_code = error.status
          var message = error._body
          this.message_to_show = message
        }
      )

    }

}