import {Component} from '@angular/core';

import {DialogRef,ModalComponent,CloseGuard} from 'angular2-modal';
import {BSModalContext} from 'angular2-modal/plugins/bootstrap';

import {Product} from '../shared/product.model';

export class ProduceDetailModalContext extends BSModalContext{
    product: Product;
}

@Component({
    selector: 'modal-content',
    templateUrl: '/products/product-detail-modal/product-detail-modal.html'
})
export class ProduceDetailModal implements CloseGuard, ModalComponent<ProduceDetailModalContext>{
    context: ProduceDetailModalContext;

    constructor(
        public dialog: DialogRef<ProduceDetailModalContext>
    ){
        this.context = dialog.context;
        // dialog.setCloseGuard(this);
    }

    onBack(){
        this.dialog.close();
    }
}