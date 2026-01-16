// Angular
import {
    OnInit,
    Component,
    Input,
    Output,
    EventEmitter } from '@angular/core';
import {
    FormBuilder,
    FormGroup,
    Validators
} from '@angular/forms';

// Plex
import { Plex } from '@andes/plex';

// Interfaces
import { IProfesional } from './../../../interfaces/IProfesional';
import { ISiisa } from './../../../interfaces/ISiisa';

// Services
import { SIISAService } from './../../../services/siisa.service';
import { ProfesionalService } from './../../../services/profesional.service';
import { EntidadFormadoraService } from './../../../services/entidadFormadora.service';
import { ProfesionService } from '../../../services/profesion.service';

@Component({
    selector: 'app-formacion-grado-form',
    templateUrl: 'formacion-grado-form.html'
})
export class FormacionGradoFormComponent implements OnInit {
    formPosgrado: FormGroup;
    activeAcc = false;
    profesiones: any[];
    profesionalGrado: any = {
        profesion: {
          nombre: null,
          codigo: null,
        },
        entidadFormadora: {
          nombre: null,
          codigo: null,
        },
        titulo: null,
        fechaEgreso: null,
        fechaTitulo: null,
        revalida: true,
        papelesVerificados: false,
        matriculacion: null,
      };
            // matriculacion: [{
            //     matriculaNumero: null,
            //     libro: null,
            //     folio: null,
            //     inicio: null,
            //     fin: null,
            //     revalidacionNumero: null,
            // }],


    @Input() profesional: IProfesional;
    @Output() submitPosgrado = new EventEmitter();

    constructor(private _fb: FormBuilder,
        private _siisaSrv: SIISAService,
        private _profSrv: ProfesionalService,
        private _entidadFormadoraService: EntidadFormadoraService,
        private plex: Plex,
       private _profesionService: ProfesionService, private _profesionalService: ProfesionalService, ) {}

    ngOnInit() {
        if (this.profesional) {
            this.profesiones = this.profesional.formacionGrado.map((value) => {
                return value.profesion;
            });
        }
    }

    onSubmit($event, form) {
        console.log(this.profesional);
     this.profesional.formacionGrado.push(this.profesionalGrado);

        this._profesionalService.putProfesional(this.profesional)
         .subscribe(resp => {
         });

    }


    loadEntidadesFormadoras(event) {
        this._entidadFormadoraService.getEntidadesFormadoras().subscribe(event.callback);
    }

    loadProfesiones(event) {
        this._profesionService.getProfesiones().subscribe(event.callback);
      }
}