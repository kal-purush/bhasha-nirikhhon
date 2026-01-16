import { Component, OnInit, ViewChild } from '@angular/core';
import { ServicesProvider } from '../../../config/services'
import {ModalDirective} from 'ngx-bootstrap/modal';
import { FormBuilder, FormGroup, Validators, FormControl } from '@angular/forms';
import { THIS_EXPR } from '@angular/compiler/src/output/output_ast';
@Component({
  selector: 'crear-caso',
  templateUrl: 'crear-caso.component.html',
  styleUrls: ['./crear-caso.component.scss']
})
export class CrearCasoComponent implements OnInit {
  @ViewChild('modalArbol') public modalArbol: ModalDirective;
  @ViewChild('modalPregunta') public modalPregunta: ModalDirective;
  bMostrarPregunta =false;
  texto_respuesta:string
  nivel=1
  aPreguntasGeneradas=[]

  arbol: any = [{
    "id_caso": "",
    "nombre_arbol": "1. ¿LA SOLICITUD SE PRESENTA PARA ACCEDER AL BENEFICIO TRIBUTARIO POR PRIMERA VEZ?",
    "arbol": [
      {
        "nivel": "",
        "id": "",
        "pregunta": "",
        "respuesta": [],
      }
    ]
  }]

  pregunta: any = {
    "nivel": "",
    "id": "",
    "pregunta": "",
    "respuesta": [],
  }

  respuesta: any = {
    "texto_respuesta": "",
    "id_hoja": "",
    "text_hoja": "",
    "modelo": "",
    "nuevas_hojas": []
  }



  constructor(public ServicesProvider: ServicesProvider, private fb: FormBuilder) { }


  ngOnInit() {

  }
  obtenerArbol() {
    this.ServicesProvider.getjson("../../../../assets/data/proceso.json", {}).then(response => {
      this.arbol = response
      console.log(response)
    })

  }

  crearArbol() {

    

  }

  armarArbol(form){
    this.aPreguntasGeneradas.push(form.value.pregunta)
    console.log(this.aPreguntasGeneradas)
    console.log(form.value)
    this.pregunta.pregunta = form.value.pregunta
    if(this.arbol[0].arbol.length == 1){
      this.arbol[0].arbol = [this.pregunta]
    }else{
      this.arbol[0].arbol.push(this.pregunta)
    }
    console.log(this.arbol)
    
    this.bMostrarPregunta=true
    if(this.nivel != 1){
      this.nivel ++;
    }
  }

  agregarRespuesta(form){
    this.aPreguntasGeneradas = []
    this.aPreguntasGeneradas.push(form.value.pregunta)
    console.log(form.value)
    this.respuesta.texto_respuesta = this.texto_respuesta
    this.pregunta.pregunta = form.value.pregunta

    if(this.respuesta.nuevas_hojas.length == 1){
      this.respuesta.nuevas_hojas = [this.pregunta]
    }else{
      this.respuesta.nuevas_hojas.push(this.pregunta)
    }

    this.arbol[0].arbol[0].respuesta = this.respuesta
    console.log(this.arbol)
    this.nivel ++;
  }

  cambiarTextoResp(rsp){
    this.texto_respuesta = rsp

  }
  



}