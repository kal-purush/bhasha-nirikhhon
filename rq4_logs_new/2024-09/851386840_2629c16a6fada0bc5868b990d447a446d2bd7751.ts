import { Component, CUSTOM_ELEMENTS_SCHEMA, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { IonContent, IonHeader, IonTitle, IonToolbar, IonButtons, IonBackButton } from '@ionic/angular/standalone';
import { Router } from '@angular/router';
import { Peliculas } from '../../models/peliculas';
import { ApiPeliculasService } from '../../services/api-peliculas.service';
import { play, bookmark } from 'ionicons/icons';
import { addIcons } from 'ionicons';

@Component({
  selector: 'app-previsualizar-pelicula',
  templateUrl: './previsualizar-pelicula.page.html',
  styleUrls: ['./previsualizar-pelicula.page.scss'],
  standalone: true,
  imports: [IonContent, IonHeader, IonTitle, IonToolbar, CommonModule, FormsModule, IonButtons, IonBackButton],
  schemas: [CUSTOM_ELEMENTS_SCHEMA]
})
export class PrevisualizarPeliculaPage implements OnInit {

  pelicula!: Peliculas;

  constructor(
    private router: Router,
    private apiPelicula: ApiPeliculasService) {
    addIcons({ play, bookmark })
  }

  ngOnInit() {

    let id = window.location.href.split("/").pop()

    this.apiPelicula.getPeliculaById(id).subscribe((pelicula: Peliculas) => {

      if (Object.keys(pelicula).length > 4) {
        this.pelicula = pelicula;
      }
      else {
        this.router.navigate(['/']);
      }

    });
  }
}