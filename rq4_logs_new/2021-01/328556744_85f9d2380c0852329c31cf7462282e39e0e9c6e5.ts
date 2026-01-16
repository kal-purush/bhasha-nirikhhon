import { Component, OnInit } from '@angular/core';
import {AccountService} from "../../services/account.service";

@Component({
  selector: 'app-account-info',
  templateUrl: './account-info.component.html',
  styleUrls: ['./account-info.component.css']
})
export class AccountInfoComponent implements OnInit {

  constructor(private accountService: AccountService) { }

  ngOnInit(): void {
    this.user = {address: '', transferAddress: '', balance: '', amount: '', remarks: ''};
    this.getAccountAndBalance();
  }

  user: { address, transferAddress, balance, amount, remarks };

  getAccountAndBalance = () => {
    const that = this;
    this.accountService.getUserBalance().then((retAccount: any) => {
      that.user.address = retAccount.account;
      that.user.balance = retAccount.balance;
      console.log('transfer.components :: getAccountAndBalance :: that.user');
      console.log(that.user);
    }).catch(error => {
      console.log(error);
    });
  }

}