import { Component, OnInit} from '@angular/core';
import { ProductService } from '../services/producto.services';
import { Producto } from './producto';
import { NgForm } from '@angular/forms';
import { element } from 'protractor';

@Component({
    selector: 'app-producto-list',
    templateUrl: './producto-list.component.html'
  })

  export class ProductoListComponent implements OnInit{

    productList: Producto[];

    constructor(private productService: ProductService){}

    ngOnInit(){
        this.productService.getProductos()
        .snapshotChanges()
        .subscribe(item=>{
            this.productList = [];
            item.forEach(element=>{
                let x = element.payload.toJSON();
                x["$key"] = element.key;
                this.productList.push(x as Producto);
            });
        });

    }
  }