import { Component, OnInit } from '@angular/core';
import { NavController } from '@ionic/angular';

@Component({
  selector: 'app-gasfiteria',
  templateUrl: './gasfiteria.page.html',
  styleUrls: ['./gasfiteria.page.scss'],
})
export class GasfiteriaPage implements OnInit {
  profesionales = [
    {
      name: 'Alan Brito',
      rating: 4.5,
      experience: '5 años de experiencia',
      profilePicture: "/assets/img/H.png",
      description: 'Especialista en instalaciones de agua y desagües.'
    },
    {
      name: 'Elsa Pallo',
      rating: 4.7,
      experience: '8 años de experiencia',
      profilePicture: '/assets/img/M.png',
      description: 'Experta en reparación y mantenimiento de tuberías.'
    },
   
  ];
  constructor(private navCtrl: NavController) { }

  ngOnInit() {
  }
  irHome() {
    console.log('Navegando al Home');
    this.navCtrl.navigateRoot('/home');
  }

  irPerfil() {
    console.log('Navegando a Perfil');
    this.navCtrl.navigateForward('/perfil');
  }

  cerrarSesion() {
    console.log('Cerrando sesión');
    this.navCtrl.navigateRoot('/authen');
  }
  contactar(nombre: string) {
    console.log(`Contactando a ${nombre}`);
  }
}