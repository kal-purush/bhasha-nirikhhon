import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss']
})
export class HomeComponent implements OnInit {

  landing: boolean = false;
  welcome: string = 'Bienvenidos al Prontuario de Leyes';
  appname: string = 'Penshiru';
  slogan: string = 'Maneje las Leyes Eficientemente';
  subscribeMSG: string = 'Mantente Informado';

  infoText: string = 
  `Penshiru es una herramienta
  innovadora que pone
  la tecnologia al Servicio
  de las Leyes`;

  constructor() { }

  ngOnInit() {
  }


}