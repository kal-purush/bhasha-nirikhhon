import { Component } from '@angular/core';
import { Router } from '@angular/router';

import { Account } from '../models/Account';

@Component({
    moduleId: module.id,
    templateUrl: 'createnewaccount.component.html'
})

export class CreateAccountComponent {
    account: Account = new Account();
    isAccountCratinSuccess = false;
    isChecked: false;

    constructor(private router: Router) {}

    createAccount() {
        this.account.customerIdentifier = this.getRandomCustomerIDNumber();
        this.account.accountNumber = this.getRandomAccountNumber();
        this.account.creationDate = new Date();
        this.account.isManagerAccount = false;
        this.account.fee = 0;
        this.account.managerUpdatedFee = 0;
        let useraccounts: any[] = JSON.parse(localStorage.getItem('useraccounts')) || [];
        useraccounts.push(JSON.stringify(this.account));
        localStorage.setItem('useraccounts', JSON.stringify(useraccounts));
        this.isAccountCratinSuccess = true;        
    }

    getRandomCustomerIDNumber() {
        return Math.floor(Math.random() * (99999 - 10000)) + 10000;     
    }

    getRandomAccountNumber() {
        return Math.floor(Math.random() * (999999999 - 100000000)) + 100000000;     
    }
}