import { ToastComponent } from './../../shared/toast/toast.component';
import { Component, OnInit } from '@angular/core';
import { Empresa } from './empresa';
import { ActivatedRoute, Router } from '@angular/router';
import { EmpresaService } from './empresa.service';
import { Ubigeo } from '../ubigeo/ubigeo';
import { UbigeoService } from '../ubigeo/ubigeo.service';

@Component({
    selector: 'empresa-crear',
    templateUrl: 'empresa-crear.component.html',
    providers: [ ToastComponent]
})
export class EmpresaCrearComponent implements OnInit {
    empresa: Empresa;
    departamentoSeleccionado: string;
    provinciaSeleccionada: string;
    distritoSeleccionado: string;
    departamentos: Ubigeo[] = [];
    provincias: Ubigeo[] = [];
    distritos: Ubigeo[] = [];
    sub: any;
    constructor(private _empresaService: EmpresaService,
        private _ubigeoService: UbigeoService,
        private toast: ToastComponent,
        private route: ActivatedRoute,
        private router: Router) { }

    ngOnInit() {
        this.empresa = new Empresa;
        this.cargaDepartamento();
    }

    cargaDepartamento() {
        this.sub = this._ubigeoService
            .lisDepartamento()
            .skipWhile((p) => p === undefined)
            .subscribe(p => {
                this.departamentos = p;
                this.departamentoSeleccionado = p[0].ubigeoId;
                this.cargaPronvicia(this.departamentoSeleccionado);
            });
    }
    cargaPronvicia(ubigeoId: string) {
        this.sub = this._ubigeoService
            .lisProvinciaByDepartamento(ubigeoId)
            .skipWhile((p) => p === undefined)
            .subscribe((p) => {
                this.provincias = p;
                this.provinciaSeleccionada = p[0].ubigeoId;
                this.cargaDistrito(this.provinciaSeleccionada);
            });

    }
    cargaDistrito(ubigeoId: string) {
        this.sub = this._ubigeoService
            .lisDistritoByProvincia(ubigeoId)
            .skipWhile((p) => p === undefined)
            .subscribe(p => {
                this.distritos = p;
                this.distritoSeleccionado = p[0].ubigeoId;
        });
    }
    cboDepartamento(ubigeoId: string) {
        console.log(ubigeoId);
        this._ubigeoService
            .lisProvinciaByDepartamento(ubigeoId)
            .skipWhile( (p) => p === undefined)
            .subscribe(p => {
                this.provincias = p;
                this.provinciaSeleccionada = p[0].ubigeoId;
                this.cboProvincia(p[0].ubigeoId);
            });
    }
    gotoLisEmpresa() {
        let link = [''];
        this.router.navigate(link);
    }
    cboProvincia(ubigeoId: string) {
        this._ubigeoService
            .lisDistritoByProvincia(ubigeoId)
            .skipWhile( (p) => p === undefined)
            .subscribe( p => {
                this.distritoSeleccionado = p[0].ubigeoId;
                this.distritos = p;
            });
    }
    grabar(){
        this.empresa.ubigeoId = this.distritoSeleccionado;
        console.log(this.empresa);
        console.log(this.distritoSeleccionado);
    }
}