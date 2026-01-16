import { Overlay } from 'angular2-modal';
import { Modal } from 'angular2-modal/plugins/bootstrap';
import { Injectable, ViewContainerRef } from '@angular/core';

@Injectable()
export class ModalHandler {

    constructor(protected overlay: Overlay,
                public modal: Modal
    ) {}

    public setRootViewContainerRef(vcr: ViewContainerRef): void {
        this.overlay.defaultViewContainer = vcr;
    }

    public confirm(): void {
        this.modal.confirm()
            .size('sm')
            .showClose(true)
            .title('Delete')
            .body('Are you sure you want to delete?')
            .okBtn('Confirm')
            .okBtnClass('btn btn-info')
            .cancelBtn('Dismiss')
            .cancelBtnClass('btn btn-warning')
            .open();
    }
}