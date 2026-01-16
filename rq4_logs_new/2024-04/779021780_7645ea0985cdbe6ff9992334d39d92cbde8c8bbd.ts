import {Component, ViewChild} from '@angular/core';
import {GoogleMapsModule, MapInfoWindow, MapMarker} from "@angular/google-maps";
import {CommonModule} from "@angular/common";

@Component({
  selector: 'app-map',
  standalone: true,
  imports: [GoogleMapsModule, CommonModule],
  templateUrl: './map.component.html',
  styleUrl: './map.component.css'
})
export class MapComponent {

  cursorLatLng: google.maps.LatLngLiteral | undefined;
  center: google.maps.LatLngLiteral = {
    lat: 53.69671,
    lng: 19.96486
  };
  zoom = 14;

  //markers
  markerOptions: google.maps.MarkerOptions = {
    draggable: false
  };
  markerPositions: google.maps.LatLngLiteral[] = [];
  @ViewChild(MapInfoWindow) infoWindow: MapInfoWindow | undefined;
  infoWindowText: string = '';


  constructor() {}
  ngOnInit(): void {}



  onMapClick($event: google.maps.MapMouseEvent | google.maps.IconMouseEvent) {
    //this.moveMap($event);
    this.addMarker($event);
  }

  onMapMousemove($event: google.maps.MapMouseEvent) {
    this.getCursorLatLng($event);

  }

  onMarkerClick(marker: MapMarker) {
    this.infoWindowText = (marker.getPosition() ?? '').toString();
    if (this.infoWindow != undefined) this.infoWindow.open(marker);
  }

  //helpers

  addMarker(event: google.maps.MapMouseEvent) {
    if (event.latLng != null) this.markerPositions.push(event.latLng.toJSON());
  }

  //moves map so that the center is in the clicked point
  moveMap(event: google.maps.MapMouseEvent) {
    if (event.latLng != null) this.center = (event.latLng.toJSON());
  }

  //fetches the current cursor's coordinates
  getCursorLatLng(event: google.maps.MapMouseEvent) {
    if (event.latLng != null) this.cursorLatLng = event.latLng.toJSON();
  }


}