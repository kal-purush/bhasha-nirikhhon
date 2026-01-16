import { Component, OnInit } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { Patologia } from 'src/app/models/patologia.model';
import { PatologiasService } from 'src/app/services/patologias.service';
import { PatologiasComponent } from '../patologias/patologias.component';

@Component({
  selector: 'app-agregar-patologia',
  templateUrl: './agregar-patologia.component.html',
})
export class AgregarPatologiaComponent implements OnInit {

  nombre:         string;
  area:           string;
  general:        string;
  descripcion:    string;
  
  constructor(private modalService: NgbModal
              , private _patologiaService: PatologiasService  
              , private _patologiasComponent: PatologiasComponent        
    ) { }

  ngOnInit() {
  }

  open(content) {
    this.modalService.open(content, { size: 'lg' });
  }

  guardar(c){
    const newPatologia: Patologia={
      nombre:         this.nombre,
      area:           this.area,
      general:        this.general,
      descripcion:    this.descripcion,
      fotos:          [],
      videos:         [],
      tags:           [],
      audios:         [],
      referencias:    ""
    }

    this._patologiaService.addPatologia(newPatologia).subscribe(p=>{
      this._patologiasComponent.load();
    });


    this.nombre="";
    this.area="";
    this.general="";
    this.descripcion="";    
    c.close();
  }

}