import { Component, OnInit,ViewChild } from '@angular/core';
import {MatTableDataSource} from '@angular/material/table';
import {MatPaginator} from '@angular/material/paginator';
import {MatSort} from '@angular/material/sort';

//Step 1) Importar los modelos y servicios
import {Sexo} from '../../models/sexo'
import{SexoService}from '../../services/sexo.service'


@Component({
  selector: 'app-mantenimientos',
  templateUrl: './mantenimientos.component.html',
  styleUrls: ['./mantenimientos.component.css']
})
export class MantenimientosComponent implements OnInit {
//Step 2-Crear un arreglo de las columnas de la tabla
displayedColumnsSexo: string[] = ['sexo', 'actions'];
//Step 3-Crear datasource para la tabla del tipo del modelo
dataSourceSexo:MatTableDataSource<Sexo>
//Step 4-Crear arreglo de tipo any para luego llenarlo y darselo al datasource
sexos: any=[];
//Step5-Crear objeto del dtipo del modelo, este se llenara para meterlo a la DB
sexo: Sexo=new Sexo();

@ViewChild(MatPaginator) paginator: MatPaginator;
@ViewChild(MatSort) sort: MatSort;

//Step 6- ingresar el service ne el constructor
  constructor(private sexoService:SexoService) { }

  ngOnInit() {   
    /* Step 7-Tomar el objeto de service del constructor y llamar al get para llenar
    el arreglo */
    this.sexoService.getSexo().subscribe(
        res => {
            console.log(res);
            this.sexos = res;
            // Assign the data to the data source for the table to render
            this.dataSourceSexo = new MatTableDataSource(this.sexos);
            this.dataSourceSexo.paginator = this.paginator;
            this.dataSourceSexo.sort = this.sort;
          },
          err => {
            console.log(err);
          }
    );
  }
//Step 8 crear las funciones add y delete
  addSexo(): void {
      //si esta vacio el objeto
      if(this.sexo.tipo!=" "){
       
      
    this.sexoService.saveSexo(this.sexo)
    .subscribe(res=>{
        console.log(this.sexo);
        console.log(res);
        alert('Sexo agregado con exito');
        this.ngOnInit(); //reiniciamos la tabla
        this.sexo = new Sexo();
      },
      err => {
        console.log(err);
        alert('Error al agregar sexo');
      }
    );

      }
      else{
          //hacer algo
      }
  }

  deleteSexo(nombreSexo: string): void {
    this.sexos = this.sexos.filter(obj => obj.nombre !== nombreSexo);
    this.dataSourceSexo = new MatTableDataSource(this.sexos);
  }



    }