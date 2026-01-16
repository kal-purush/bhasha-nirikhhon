import { Injectable } from '@angular/core';
import { NgbModalRef, NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { ErrorModalComponent } from '../../components/error-modal/error-modal.component';

@Injectable({
  providedIn: 'root'
})
export class ModalService {

  private modalRef: NgbModalRef;

  constructor(private modalService: NgbModal) { }

  error(message: string) {
    this.modalRef = this.modalService.open(ErrorModalComponent,  { centered: true });
    this.modalRef.componentInstance.message = message;
  }

  onSave() {
    this.modalRef.close();
  }
}