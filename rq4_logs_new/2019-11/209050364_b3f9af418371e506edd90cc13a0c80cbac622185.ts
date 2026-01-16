import { FormBuilder, Validators } from '@angular/forms';
import {Injectable} from '@angular/core';

@Injectable({
    providedIn: 'root'
})

export class FormularioService {
    constructor(private formBuilder: FormBuilder,) {}

    public createFormGrupo() {
        return this.formBuilder.group({
          nome: ['', Validators.required],
          id: ""
        });
    }

    public createFormAgenda() {
        return this.formBuilder.group({
          nome: ['', Validators.required],
          descricao: ['', Validators.required],
          tipo: ['', Validators.required],
          data_disponibilizacao: ['', Validators.required],
          data_encerramento: ['', Validators.required],
          id: ['']
        })
    }
    
    public createFormLogin() {
        return this.formBuilder.group({
            matricula: ['', Validators.required],
            senha: ['', Validators.required]
        })
    }

    public createFormRegistro() {
        return this.formBuilder.group({
            username: ['', Validators.required],
            first_name: ['', Validators.required],
            last_name: ['', Validators.required],
            password: ['', Validators.required]
        })
    }
}
