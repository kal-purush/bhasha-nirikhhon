
import { Component } from '@angular/core';
import { LoginService } from './services/login.service';
import { loggedUserService } from './services/logged-user.service';
import { Router, ActivatedRoute } from '@angular/router';

@Component({
  selector: 'index-component',
  templateUrl: './templates/indexComponent.template.html',
  styleUrls: ['./templates/css/index.css', './templates/font-awesome/css/font-awesome.css']
})

export class IndexComponent {
  title = '¡Welcome to TalkClass!';
  classrooms = [];
  currentPage = 0;
  constructor(private serviciologin: LoginService, private Ruta: ActivatedRoute, private router: Router) {
    //etc
  }

  ngOnInit() {
    if (this.serviciologin.isLogged){
        console.log("usuario logeado")
    }else{
        console.log("usuario no loggeador...redirigiendo a login")
        this.router.navigateByUrl('/login');
    }

  }

}