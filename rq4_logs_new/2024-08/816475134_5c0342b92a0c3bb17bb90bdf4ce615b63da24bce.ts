import { Component, Output, EventEmitter } from '@angular/core';
import { DataStorageService } from '../services/data-storage.service';
import { DataSharingService } from '../services/data-sharing.service';
import { NgFor } from '@angular/common';
import { OnInit } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { PersonnelEntry } from '../services/data-sharing.service';


export interface ObservationEntry {
  nombre: string;
  observacion: string; // Asegúrate de que esto sea siempre una cadena, no null.
}

@Component({
  selector: 'app-data-observation',
  standalone: true,
  imports: [NgFor, FormsModule],
  templateUrl: './data-observation.component.html',
  styleUrls: ['./data-observation.component.css']
})

export class DataObservationComponent implements OnInit {
  @Output() observationChanged = new EventEmitter<ObservationEntry[]>(); // Emite un array de observaciones

  observationEntries: ObservationEntry[] = []; // Entradas con observaciones
  names: string[] = []; // Lista de nombres disponibles

  constructor(private dataSharingService: DataSharingService) {}

  ngOnInit(): void {
    this.dataSharingService.getPersonnelManagerDataObservable().subscribe(entries => {
      this.names = entries.map(entry => entry.nombre); // Obtener nombres de entradas
    });
  }

  onObservationChange(event: Event, index: number): void {
    const value = (event.target as HTMLTextAreaElement).value; // Valor de la observación
    this.observationEntries[index].observacion = value ?? ''; // Actualizar observación local
    this.dataSharingService.updateObservation(index, value ?? ''); // Actualizar observación global
    this.observationChanged.emit(this.observationEntries); // Emitir el array de observaciones
    console.log("Observations updated", this.observationEntries);
  }

  onNameChange(event: Event, index: number): void {
    const selectedName = (event.target as HTMLSelectElement).value; // Obtener nombre seleccionado
    this.observationEntries[index].nombre = selectedName; // Actualizar nombre en la entrada local
    this.updateObservationForSelectedName(index); // Sincronizar con el servicio
    this.observationChanged.emit(this.observationEntries); // Emitir el array de observaciones
    console.log("Name updated", this.observationEntries);
  }

  private updateObservationForSelectedName(index: number): void {
    const selectedName = this.observationEntries[index].nombre; // Nombre seleccionado
    if (selectedName) {
      const data = this.dataSharingService.getPersonnelManagerData(); // Obtener datos actuales
      const entry = data.find(entry => entry.nombre === selectedName); // Buscar entrada por nombre
      if (entry) {
        this.observationEntries[index].observacion = entry.observacion ?? ''; // Actualizar observación
        entry.observacion = this.observationEntries[index].observacion; // Sincronizar con el servicio
        this.dataSharingService.setPersonnelManagerData(data); // Actualizar datos
        console.log("Updated observation", this.observationEntries);
        console.log("Updated data", data);
      }
    }
  }

  addObservationEntry(): void {
    // Filtrar nombres que ya están seleccionados en las entradas actuales
    const selectedNames = this.observationEntries.map(entry => entry.nombre).filter(name => name);
    // Agregar una nueva entrada vacía para el nombre y la observación
    this.observationEntries.push({ nombre: '', observacion: '' });
  }

  getAvailableNames(index: number): string[] {
    // Filtrar nombres disponibles que no están ya seleccionados en otras entradas
    const selectedNames = this.observationEntries
      .map((entry, idx) => idx !== index ? entry.nombre : null)
      .filter(name => name !== null);
    return this.names.filter(name => !selectedNames.includes(name));
  }
}