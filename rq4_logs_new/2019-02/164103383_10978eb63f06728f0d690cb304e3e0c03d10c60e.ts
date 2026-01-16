import { Component, OnInit, Input } from '@angular/core';
import { NgxSmartModalService } from 'ngx-smart-modal';

@Component({
  selector: 'app-depository-wishlist',
  templateUrl: './depository-wishlist.component.html',
  styleUrls: ['./depository-wishlist.component.scss']
})
export class DepositoryWishlistComponent implements OnInit {

  @Input()
  isLoggedInToBookDepository: boolean;

  model = {
    email: '',
    password: ''
  };
  isShowPassword = false;

  constructor(public ngxSmartModalService: NgxSmartModalService) {
  }

  ngOnInit() {
  }

}