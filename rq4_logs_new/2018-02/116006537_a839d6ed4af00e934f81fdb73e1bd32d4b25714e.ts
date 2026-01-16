import { Component, Input, OnInit } from '@angular/core';
import { LoginService } from './services/login.service';
import { Router, ActivatedRoute } from '@angular/router';
import { Multimedia } from './classes/multimedia.class';

@Component({
  selector: 'multimedia-lists',
  templateUrl: './templates/multimedia-list.template.html',
  styleUrls: ['./templates/css/eventdetails.css', './templates/css/sidemenu.css', './templates/font-awesome/css/font-awesome.css']
})

export class MultimediaListComponent implements OnInit{
  @Input() listamultimedia: Multimedia[];
  constructor(private login: LoginService, private router: Router) {
    //etc
  }
  ngOnInit(){
    if(!this.login.isLogged){
      this.router.navigateByUrl("/login");
    } 
  }
  //El componente recibe una lista de multimedia y las muestra

}