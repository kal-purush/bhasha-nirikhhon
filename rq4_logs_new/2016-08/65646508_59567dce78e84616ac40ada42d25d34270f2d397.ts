import { Component, OnInit } from '@angular/core';

import { DataService } from '../shared/data.service'

@Component({
    selector: 'clients',
    template: require('./clients.comp.html'),
    styles: [require('./clients.comp.scss'), require('../../styles/main.scss')],
    providers: [DataService],
})
export class ClientsComponent implements OnInit {
    private clients: any;

    constructor(private dataService: DataService){
    }

    ngOnInit () {
        this.clients = this.dataService.getClients();
    }

    ngAfterContentInit () {
    }
}