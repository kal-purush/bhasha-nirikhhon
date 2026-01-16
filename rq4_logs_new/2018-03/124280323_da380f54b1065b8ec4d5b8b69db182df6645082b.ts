import { Component, OnInit } from '@angular/core';
import { AdministradorService } from '../administrador.service';
import { MatDialog, MatSnackBar } from '@angular/material';
import { ModalAddCurso } from './modalAddCurso/modalAddCurso.component';
import { Curso,Paralelo,Grado,Grupo,Turno } from '../modelos/grupo';

@Component({
  selector: 'app-cursos-layout',
  templateUrl: './cursos-layout.component.html',
  styleUrls: ['./cursos-layout.component.css']
})
export class CursosLayoutComponent implements OnInit {

  idTurnoActual:number;
    
  turnos:Turno[];
  grados:Grado[];
  grupos:Grupo[];
  paralelos:Paralelo[];
  cursos:Curso=new Curso();

  constructor(
    private serve:AdministradorService,
    public dialog:MatDialog,
    private notificaciones:MatSnackBar
  ) { }

  ngOnInit() {
    this.getTurnos();
    this.getGrados();
    this.getGrupos();
    this.getParalelos(); 
  }
  openModal() {
    this.dialog.open(ModalAddCurso, {
      width: '330px',height:'555px',
      data: {
        animal: 'panda',
        idTurno:this.idTurnoActual,
        paralelos:this.paralelos,
        turnos:this.turnos,
        grupos:this.grupos,
        grados:this.grados
      }
    });
  }
  cambioCursos(id){
    this.getCursos(id)
  }

  AbrirNotificacion(mensaje:string,action:string){
    this.notificaciones.open(mensaje,action,{
      duration:1000
    })
  }
  getCursos(id){
    this.serve.getCursoTurno(id).subscribe((data:Curso)=>{
      if(data){
        this.cursos=data;
        console.log(data)
      }else{
        this.AbrirNotificacion("","");
      }    
    },err=>{
      this.AbrirNotificacion("Error de conexion","");
    })
  }
  getParalelos(){
    this.serve.getParalelo().subscribe(data=>{
      this.paralelos=data;
    })
  }
  getGrupos(){
    this.serve.getGrupo().subscribe(data=>{
      this.grupos=data;
    })
  } 
  getGrados(){
    this.serve.getGrado().subscribe(data=>{
      this.grados=data;
    })
  }
  getTurnos(){
    this.serve.getTurno().subscribe(data=>{
      this.turnos=data;
      if(this.turnos.length>0){}
        this.idTurnoActual=this.turnos[0].id
        this.getCursos(this.idTurnoActual)
    })
  }
}
