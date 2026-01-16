import { Component, Input, OnInit } from '@angular/core';
import { LoginService } from './services/login.service';
import { UsersService } from './services/users.service';
import { EventsService } from './services/events.service';


import { Router, ActivatedRoute } from '@angular/router';
import { User } from './classes/user.class';
import { Event } from './classes/evento.class';

@Component({
  selector: 'search-result',
  templateUrl: './templates/search-result.template.html',
  styleUrls: ['./templates/css/listausuarios.css', './templates/font-awesome/css/font-awesome.css']
})

export class SearchResultComponent implements OnInit{
  //@Input() userlist: User[];
  public key: string;
  public type : string;
  public users: User[] = [];
  public events: Event[] = [];
  constructor(private ServicioLogin: LoginService, private ServicioEventos: EventsService, private ServicioUsers: UsersService ,private router: ActivatedRoute, private route: Router) {
    //etc
  }
  ngOnInit(){
    console.log('al menos entro')
    if(!this.ServicioLogin.isLogged){
      this.route.navigateByUrl("/login");
    } else{
      this.router.queryParams
      .subscribe(params => {
        // Defaults to 0 if no query param provided.
        this.type = params['type'];
        this.key = params['value'].toLowerCase();
        if (this.type == 'Eventos'){
          this.filter_events()
        }
        if(this.type=='Usuarios'){
          this.filter_users()
        }
        console.log('Query param page: ', this.type, this.key);
      });
    }
  }
  filter_users(){
    this.ServicioUsers.get_users().then(
      result =>{
        for (let user of result){
          if (user.username.toLowerCase().includes(this.key) || user.email.toLowerCase().includes(this.key) ||
          user.first_name.toLowerCase().includes(this.key) || user.last_name.toLowerCase().includes(this.key) ){
            this.users.push(user)
          }
        }
      }
    )
  }

  filter_events(){
    this.ServicioEventos.get_events().then(
      result =>{
        for (let event of result){
          if (event.title.toLowerCase().includes(this.key) || event.description.toLowerCase().includes(this.key) ||
          event.place.toLowerCase().includes(this.key)  ){
            this.events.push(event)
          }
        }
      }
    )
  }
  //El componente recibe una lista de multimedia y las muestra

}