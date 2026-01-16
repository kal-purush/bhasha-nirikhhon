import {Component} from '@angular/core';
import {ProductsService} from 'src/services/products.service';
import {ProductComponent} from './product/product.component';
import {MdButton} from '@angular2-material/button';

const template = require('./app.tpl.html') as string;

@Component({
    selector: 'warehouse-app',
    template,
    directives: [ProductComponent, MdButton],
    providers: [ProductsService]
})
export class AppComponent {
    products:any;
    constructor(private _producsService:ProductsService) {
        this.products = _producsService.getAll();
    }
}