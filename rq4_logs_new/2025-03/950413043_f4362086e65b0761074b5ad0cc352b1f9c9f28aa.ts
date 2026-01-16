// src/app/components/historial/historial.component.ts
import { Component, OnInit } from '@angular/core';
import { HistorialService } from '../../services/Historial.service';
import { Location } from '@angular/common';

@Component({
  selector: 'app-historial',
  templateUrl: './historial.component.html',
  styleUrls: ['./historial.component.css']
})
export class HistorialComponent implements OnInit {
  historial: any[] = [];

  constructor(private historialService: HistorialService,private location: Location) {}

  ngOnInit(): void {
    this.historialService.getHistorial().subscribe((data) => {
      this.historial = data;
    });
  }
  regresar(): void {
    this.location.back();  // Regresar a la página anterior
  }

}