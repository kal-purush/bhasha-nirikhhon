import {Injectable} from '@angular/core';
import {MessageModalComponent} from '../message-modal/message-modal.component';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';
import {Router} from '@angular/router';

@Injectable({
  providedIn: 'root'
})
export class ModalMessageService {

  private modal: NgbModal;

  constructor(modalService: NgbModal) {
    this.modal = modalService;
  }

  show(title: string, body: string) {
    const modalRef = this.modal.open(MessageModalComponent, {centered: true});
    modalRef.componentInstance.title = title;
    modalRef.componentInstance.body = body;
  }
}