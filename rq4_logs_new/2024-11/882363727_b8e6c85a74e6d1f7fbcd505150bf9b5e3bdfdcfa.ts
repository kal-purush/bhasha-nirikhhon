import { Component, OnInit } from '@angular/core';
import { NavController } from '@ionic/angular';

@Component({
  selector: 'app-homep',
  templateUrl: './homep.page.html',
  styleUrls: ['./homep.page.scss'],
})
export class HomepPage implements OnInit {
  constructor(private navCtrl: NavController) {}

  ngOnInit() {}

  cerrarSesion() {
    this.navCtrl.navigateRoot('/authen');
  }

  verPerfil() {
    console.log("Perfil de usuario");
  }

  verServicios() {
    console.log("Trabajos por realizar");
  }

  verCalificaciones() {
    console.log("Calificaciones");
  }
}