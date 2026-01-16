import { Component, ElementRef, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router'; // Para obtener el parámetro ID de la URL
import { EventosService } from 'src/app/data/Services/eventos.service'; // Asegúrate de usar la ruta correcta
import { ServiciosService } from 'src/app/data/Services/servicio.service'; // Asegúrate de usar la ruta correcta
import { Evento } from 'src/app/data/Interfaces/evento'; // Modelo de Evento (asegúrate de que esté correctamente definido)
import { Servicio } from 'src/app/data/Interfaces/servicio'; // Modelo de Evento (asegúrate de que esté correctamente definido)
import Swal from 'sweetalert2';

@Component({
  selector: 'app-detaller-evento-cliente',
  templateUrl: './detaller-evento-cliente.component.html',
  styleUrls: ['./detaller-evento-cliente.component.css']
})
export class DetallerEventoClienteComponent implements OnInit {
  eventos: Evento[] = []; // Para almacenar los eventos
  idCliente!: string; // ID del cliente obtenido
  servicio!: Servicio; // Servicio asociado a un evento
  detallesAbiertos: boolean[] = [];
  constructor(
    private elRef: ElementRef,
    private eventoService: EventosService, // Inyección del servicio
    private serveService: ServiciosService, // Inyección del servicio
    private route: ActivatedRoute // Para obtener parámetros de la URL
  ) {}

  ngOnInit(): void {
    // Obtener el ID del cliente desde la URL
    const storedUser = sessionStorage.getItem('user');
    console.log(this.idCliente)
    if (storedUser) {
      const user = JSON.parse(storedUser);
      this.idCliente = user.id; // Asignar el id del cliente desde sessionStorage, asegúrate de que este campo esté disponible
    }

    this.route.paramMap.subscribe(params => {

      if (this.idCliente) {
        this.cargarEventos(this.idCliente); // Cargar los eventos del cliente
      }
    });
  }
  cargarEventos(idCliente: string): void {
    this.eventoService.getEventosByCliente(idCliente).subscribe({
      next: (eventos: Evento[]) => {
        this.eventos = eventos; // Guardar los eventos obtenidos
        if (eventos.length > 0) {
          const idServicio = eventos[0].idServicio ?? ''; // Valor predeterminado si es undefined
          if (idServicio) {
            this.cargarServicio(idServicio);
          } else {
            console.warn('El evento no tiene un idServicio asociado.');
          }
        }
      },
      error: (error) => {
        console.error('Error al obtener eventos:', error);
      },
    });
  }
  
  cargarServicio(idServicio: string): void {
    this.serveService.getServicioById(idServicio).subscribe({
      next: (servicio: Servicio) => {
        this.servicio = servicio; // Guardar el servicio obtenido
      },
      error: (error) => {
        console.error('Error al obtener el servicio:', error);
      }
    });
  }
  toggleDetalles(index: number): void {
    this.detallesAbiertos[index] = !this.detallesAbiertos[index];
  }
  cancelarReserva(eventoId: string): void {
    // Encuentra el evento en la lista
    const evento = this.eventos.find(e => e.id === eventoId);
  
    if (evento) {
      // Crea un nuevo objeto con el estado actualizado
      const eventoActualizado: Evento = { 
        ...evento, 
        estado: 'cancelado' as 'cancelado' 
      };
  
      // Llama al servicio para actualizar
      this.eventoService.updateEvento(eventoId, eventoActualizado).subscribe({
        next: (eventoActualizado) => {
          console.log(`Reserva cancelada para el evento con ID: ${eventoId}`);
          
          // Actualiza la lista local de eventos
          this.eventos = this.eventos.map(e =>
            e.id === eventoId ? eventoActualizado : e
          );
  
          // Muestra el mensaje de confirmación
          Swal.fire({
            icon: 'success',
            title: 'Reserva Cancelada',
            text: 'La reserva se ha cancelado exitosamente.',
            confirmButtonColor: '#3085d6',
            confirmButtonText: 'Aceptar'
          });
        },
        error: (error) => {
          console.error('Error al cancelar la reserva:', error);
  
          // Muestra un mensaje de error
          Swal.fire({
            icon: 'error',
            title: 'Error',
            text: 'Hubo un problema al cancelar la reserva. Intenta nuevamente.',
            confirmButtonColor: '#d33',
            confirmButtonText: 'Aceptar'
          });
        },
      });
    } else {
      console.warn(`No se encontró un evento con ID: ${eventoId}`);
  
      // Muestra un mensaje de advertencia si el evento no se encuentra
      Swal.fire({
        icon: 'warning',
        title: 'Evento No Encontrado',
        text: 'No se encontró un evento con el ID especificado.',
        confirmButtonColor: '#ffc107',
        confirmButtonText: 'Aceptar'
      });
    }
  }
  
  
}