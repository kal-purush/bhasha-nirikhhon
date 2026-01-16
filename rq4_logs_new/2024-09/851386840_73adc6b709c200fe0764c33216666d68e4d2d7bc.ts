import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { IonContent, IonHeader, IonTitle, IonToolbar, IonButtons, IonBackButton } from '@ionic/angular/standalone';
import { ActivatedRoute } from '@angular/router';
import { Pelicula } from 'src/app/services/ContinuarViendoPeliculas';
import { ContinuarViendoPeliculas } from 'src/app/services/api-continuarviendo.service';

@Component({
  selector: 'app-continuar-viendo',
  templateUrl: './continuar-viendo.page.html',
  styleUrls: ['./continuar-viendo.page.scss'],
  standalone: true,
  imports: [IonContent, IonHeader, IonTitle, IonToolbar, CommonModule, FormsModule, IonButtons, IonBackButton]
})
export class ContinuarViendoPage implements OnInit {

  constructor(
    private route: ActivatedRoute,
    private continarViendoPelicula: ContinuarViendoPeliculas
  ) { }

  pelicula: Pelicula | undefined;

  // Cuando se inicie la vista ejecutamos:
  ngOnInit() {
    const id = Number(this.route.snapshot.paramMap.get('id'));
    this.pelicula = this.continarViendoPelicula.getPeliculas().find(peli => peli.id === id);
  }

}