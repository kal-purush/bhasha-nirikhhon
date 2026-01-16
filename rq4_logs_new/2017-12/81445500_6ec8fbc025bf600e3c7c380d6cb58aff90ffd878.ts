import { Component, OnInit, Output, Input, EventEmitter } from '@angular/core';
import { Plex } from '@andes/plex/src/lib/core/service';
// import { PlexValidator } from 'andes-plex/src/lib/core/validator.service';
import * as Enums from './../../utils/enumerados';
import { FormBuilder, FormGroup, Validators, FormArray } from '@angular/forms';

// Services
import { NumeracionMatriculasService } from './../../services/numeracionMatriculas.service';
import { ProfesionService } from './../../services/profesion.service';

@Component({
    selector: 'app-numeracion-form-matriculas',
    templateUrl: 'numeracion-matriculas-form.html'
})
export class NumeracionMatriculasFormComponent implements OnInit {
    private formNumeracion: FormGroup;
    private numeracionMatricula: any = {
        profesion: null,
        proximoNumero: null
    };


    constructor(private _numeracionesService: NumeracionMatriculasService,
        private _profesionService: ProfesionService,
        private _formBuilder: FormBuilder,
        private plex: Plex) {

    }

    ngOnInit() {

    }

    guardarNumeracion($event) {
        if ($event.formValid) {
        this._numeracionesService.saveNumeracion(this.numeracionMatricula)
        .subscribe((resp) => {
            if (resp) {
                this.plex.toast('success', 'Se guardo con exito!', 'informacion', 1000);
            }
        });
    }
    }

    loadProfesiones(event) {
        this._profesionService.getProfesiones().subscribe(event.callback);
    }

    volverAlListado() {

    }
}