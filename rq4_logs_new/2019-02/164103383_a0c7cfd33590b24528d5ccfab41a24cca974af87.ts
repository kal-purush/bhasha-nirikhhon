import { Component, OnInit } from '@angular/core';
import { WishlistActions } from 'src/app/model/actions/wishlist.actions';
import { Observable } from 'rxjs';
import { WishlistState } from 'src/app/model/state';
import { select } from '@angular-redux/store';

@Component({
  selector: 'app-wishlist',
  templateUrl: './wishlist.component.html',
  styleUrls: ['./wishlist.component.scss']
})
export class WishlistComponent implements OnInit {

  @select() wishlist: Observable<WishlistState>;

  constructor(private wishlistActions: WishlistActions) {
  }

  ngOnInit() {
    this.wishlistActions.fetchWishlist();
  }

}