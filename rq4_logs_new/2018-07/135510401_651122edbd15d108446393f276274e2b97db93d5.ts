import { Component } from '@angular/core';
import { AuthService } from '../services/auth.service';
import { Router } from '@angular/router';
import { environment } from '../../../environments/environment';
import { AppMessageService } from "../../app-message.service";

import * as jQuery from 'jquery';
@Component({
    templateUrl: './registry.component.html',
})
export class RegistryComponent {
    user: any = {
        username: null,
        password: null,
        repeatPassword:null,
        name:null
    };

    constructor(private AuthService: AuthService, private router: Router, private notification: AppMessageService) {
    }

    save(e) {
        e.preventDefault();
        this.showLoading();
        if (!this.user.username || !this.user.password || !this.user.repeatPassword) {
            return;
        }
        let data = {
            "name":this.user.name,
            "email":this.user.username,
            "password":this.user.password
        };

        this.AuthService.registry(data)
            .then((res) => {
               this.hideLoading();
               this.notification.message('Sucesso','Usuario cadastrado com sucesso','success');
               this.router.navigate(['user/login']);
            }).catch(() => {
                this.notification.message('Erro','Não foi possivel cadastrar usuario','error');
                this.hideLoading();
            });
    }

    login()
    {
        this.router.navigate(['user/login']);
    }

    hideLoading(){
        jQuery("#bifrostBarSpinner").hide();
    }
    showLoading(){
        jQuery("#bifrostBarSpinner").show();
    }
}