import { Component, OnInit } from '@angular/core';
import { HttpClient, HttpHeaders, HttpClientJsonpModule, HttpResponse } from '@angular/common/http';

@Component({
  selector: 'app-coupon',
  templateUrl: './coupon.component.html',
  styleUrls: ['./coupon.component.css']
})
export class CouponComponent implements OnInit {
  private _urlGetAll = "http://localhost:8080/admin/companies"
  public companies = [];
  private data;

  constructor(private http: HttpClient) { }

  ngOnInit() {
    let resp = this.http.get(this._urlGetAll);
    resp.subscribe((data) => console.log(data));
    //Get All Companies 
    this.http.get(this._urlGetAll).subscribe((data: any[]) => {
      this.companies = data;
    });
  }
}