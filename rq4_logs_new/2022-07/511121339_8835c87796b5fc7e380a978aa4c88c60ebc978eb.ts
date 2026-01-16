import {Component, OnInit} from '@angular/core';
import {User} from "../models/user";
import {CheckRegFormService} from "../services/check-reg-form.service";

import {FlashMessagesService} from "angular2-flash-messages";

@Component({
    selector: 'app-reg',
    templateUrl: './reg.component.html',
    styleUrls: ['./reg.component.scss']
})
export class RegComponent implements OnInit {
    player!: User;

    constructor(
        private checkRegFormService: CheckRegFormService,
        private messageService: FlashMessagesService,
    ) {
    }

    ngOnInit(): void {
        this.player = {
            name: '',
            login: '',
            email: '',
            password: '',

            balance: 100,
            point: 0,
        };
    }

    registerNewPlayer() {
        // простая проверка на заполненность формы (со временем можно добавить валидацию по каждому полю)
        if (!this.checkRegFormService.checkPlayer(this.player)) {
            this.messageService.show('Неверно заполнена форма: ', {
                cssClass: 'alert-danger',
                timeout: 4000
            });
            return;
        }

        return true;
    }
}