import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { CacheService } from 'src/libs/service/request/cache.service';

@Component({
  selector: 'app-payment-success',
  templateUrl: './payment-success.component.html',
  styleUrls: ['./payment-success.component.scss']
})
export class PaymentSuccessComponent implements OnInit{
  constructor(
    private router: Router,
    private cacheService: CacheService,
    ){
    
  }
  ngOnInit(): void {
    
  }

  onClick(){
    this.cacheService.remove('listIdGhct');
    this.router.navigate(['/'])
  }
}