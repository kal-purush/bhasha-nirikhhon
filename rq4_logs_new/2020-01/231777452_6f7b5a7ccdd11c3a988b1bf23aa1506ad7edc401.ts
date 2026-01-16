import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router'
import { ConfirmationService, Message } from 'primeng/api';
import { ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';
import { BaseComponent } from '../../common/base.component';
import { Address } from '../../models/address';
import { AddressesService } from './service/addresses.service';

@Component({
    templateUrl: './addresses.component.html'
})

export class AddressesComponent extends BaseComponent implements OnInit {
    constructor(
        private activatedRoute: ActivatedRoute,
        private location: Location,
        private addressesService: AddressesService,
        private router: Router,
    ) { super(activatedRoute, location) };

    address: Array<Address> = new Array<Address>();
    pageTitle: string = "Lista lokalizacji";

    ngOnInit(): void {

        this.downloadAddresses();
    }
    downloadAddresses(): void {
        this.addressesService.getAll().subscribe(
            address => this.address = address,
            errorMessage => this.showMessage(true, 'warn', 'Information', false, errorMessage)
        );

    }
    getAddress(id: number): void {
        this.router.navigate(['./addresses/address-details', id]);
    }

    updateAddress(id: number): void {
        this.router.navigate(['./addresses/address-update', id]);
    }

}

