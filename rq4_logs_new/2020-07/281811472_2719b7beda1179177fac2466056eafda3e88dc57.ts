import { Component, OnInit } from '@angular/core';
import { Cliente } from "./cliente";

@Component({
  selector: 'app-clientes',
  templateUrl: './clientes.component.html'
})
export class ClientesComponent implements OnInit {

  clientes: Cliente[] = [
    {id: 1, nombre: 'Adrián', apellido: 'González', email: 'adrian@gmail.com', createAt: '2020-12-11'},
    {id: 2, nombre: 'Gregorio', apellido: 'Cobá', email: 'gregorio@gmail.com', createAt: '2020-07-23'},
    {id: 3, nombre: 'Juan', apellido: 'Pérez', email: 'juan@gmail.com', createAt: '2020-06-05'}
  ];

  constructor() { }

  ngOnInit(): void {
  }

}