import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { AuctionSelectService } from './auction-select.service';

@Injectable({
  providedIn: 'root'
})
export class BiddingService {

  constructor() { }

  public canBid = false;
  private canBidMessageSource = new BehaviorSubject<boolean>(this.canBid)
  public canBidMessage = this.canBidMessageSource.asObservable();

  public bidAmount = 0;
  private bidAmountMessageSource = new BehaviorSubject<number>(this.bidAmount)
  public bidAmountMessage = this.bidAmountMessageSource.asObservable();

  changeCanBidMessage(message : boolean)
  {
    this.canBidMessageSource.next(message);
  }

  changeCurrentAmount(message : number)
  {
    this.bidAmountMessageSource.next(message);
  }
}