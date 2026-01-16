import { Component, OnInit } from '@angular/core';
import {
  geoJSON,
  icon,
  ImageOverlay,
  imageOverlay,
  Map,
  Marker,
  tileLayer,
} from 'leaflet';

@Component({
  selector: 'map',
  templateUrl: './map.component.html',
  styleUrls: ['./map.component.css'],
})
export class MapComponent implements OnInit {
  private geojsonFeature: any = [
    {
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [-92.63844, 16.73744],
      },
      properties: {
        name: 'Coors Field', // Nombre del punto
        amenity: 'Baseball Stadium',
        popupContent: 'En java!',
      },
    },
    {
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [-93.1292, 16.7569],
      },
      properties: {
        name: 'Coors Field', // Nombre del punto
        amenity: 'Baseball Stadium',
        popupContent: 'Hola!',
      },
    },
    {
      type: 'Feature',
      geometry: {
        coordinates: [-92.64098845222752, 16.72706424268783],
        type: 'Point',
      },
      properties: {
        name: 'Coors Field', // Nombre del punto
        amenity: 'Baseball Stadium',
        popupContent: 'Mundo!',
      },
    },
  ];

  ngOnInit(): void {
    setTimeout(() => {
      const map = new Map('map').setView([16.7569, -93.1292], 8);
      tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(
        map
      );

      this.geojsonFeature.forEach(
        (feature: {
          geometry: { coordinates: any };
          properties: { popupContent: any };
        }) => {
          const coordinates = feature.geometry.coordinates;
          const popupContent = feature.properties.popupContent;

          // Crear el ícono personalizado
          const customIcon = icon({
            iconUrl: 'logo.png', // URL de un ícono personalizado (puedes poner una URL a una imagen)
            iconSize: [25, 41], // Tamaño del ícono
            iconAnchor: [12, 41], // Punto de anclaje del ícono (en el fondo)
            popupAnchor: [1, -34], // Donde aparece el popup
          });

          // Crear marcador
          const marker = new Marker([coordinates[1], coordinates[0]], {
            icon: customIcon,
          })
            .bindPopup(popupContent) // Asignar el contenido del popup
            .addTo(map);

          marker.on('click', () => {
            alert('Hola Mundo');
          });
        }
      );
      map.invalidateSize();
    }, 0);
  }
}