import { Component, OnInit, ViewChild } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { Location } from '@angular/common';
import { MatSort, MatTableDataSource } from '@angular/material';
import { take } from 'rxjs/operators';
import { DataApiService } from '../../../../services/data-api.service';
import { PdfService } from '../../../../services/pdf.service';

@Component({
  selector: 'app-shopping-list',
  templateUrl: './shopping-list.component.html',
  styleUrls: ['./shopping-list.component.css']
})
export class ShoppingListComponent implements OnInit {
  orderColumns: string[] = ['id', 'clientName'];
  orderList: MatTableDataSource<any> = new MatTableDataSource([]);
  displayedColumns: string[] = ['id', 'name', 'description', 'category', 'quantity', 'unit'];
  productList: MatTableDataSource<any> = new MatTableDataSource([]);
  @ViewChild(MatSort) orderSort: MatSort;
  @ViewChild(MatSort) productSort: MatSort;

  constructor(
    private _route: ActivatedRoute,
    private _router: Router,
    private _location: Location,
    private _dataApi: DataApiService,
    private _pdfSvc: PdfService
  ) { }

  ngOnInit() {
    this._route.data
      .pipe(take(1))
      .subscribe(routeData => {
        if (routeData['orders']) {
          this._setTableDataSource(routeData['orders']);
        }
    });
  }

  ngOnDestroy() {
  }

  close() {
    if (window.history.length > 1) {
      this._location.back();
    } else {
      this._router.navigate(['../open'], { relativeTo: this._route });  // navigate to '/orders/open'
    }
  }

  async generatePDF() {
    this._pdfSvc.downloadPDF('Order', 'getShoppingListInPdf', [this.orderList.data.map(order => order.id)])
    .pipe(take(1))
    .subscribe(pdfFile => {
      const element = document.createElement('a');
      element.href = URL.createObjectURL(pdfFile);
      element.download =  `shopping_list.pdf`;
      // Firefox requires the element to be in the body
      document.body.appendChild(element);
      //simulate click
      element.click();
      //remove the element when done
      document.body.removeChild(element);
    });
  }

  private _setTableDataSource(orders) {
    this.orderList = new MatTableDataSource(orders.map(function(order) {
      return {
        id: order.id,
        clientName: order.client.name
      }
    }));
    this.orderList.sort = this.orderSort;

    let productMap = this._getAggregateProductCount(orders);
    this.productList = new MatTableDataSource(Array.from(productMap.values()));
    this.productList.sort = this.productSort;
  }

  private _getAggregateProductCount(orders) {
    let productMap = new Map();
    orders.forEach(function(order) {
      order.orderItem.forEach(function(orderItem) {
        if (!productMap.has(orderItem.productId)) {
          const product = orderItem.product;
          productMap.set(product.id, {
            id: product.id,
            name: product.name,
            description: product.description,
            category: product.category,
            unit: product.unit,
            imageUrl: product.settings.imageUrl,
            quantity: Number(orderItem.quantity)
          });
        } else {
          let productValue = productMap.get(orderItem.productId);
          productValue.quantity += Number(orderItem.quantity);
        }
      })
    })
    return productMap;
  }
}