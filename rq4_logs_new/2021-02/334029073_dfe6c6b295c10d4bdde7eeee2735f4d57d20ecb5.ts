import { Component } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { JhiEventManager } from 'ng-jhipster';

import { IEstoque } from 'app/shared/model/estoque.model';
import { EstoqueService } from './estoque.service';

@Component({
  templateUrl: './estoque-delete-dialog.component.html',
})
export class EstoqueDeleteDialogComponent {
  estoque?: IEstoque;

  constructor(protected estoqueService: EstoqueService, public activeModal: NgbActiveModal, protected eventManager: JhiEventManager) {}

  cancel(): void {
    this.activeModal.dismiss();
  }

  confirmDelete(id: number): void {
    this.estoqueService.delete(id).subscribe(() => {
      this.eventManager.broadcast('estoqueListModification');
      this.activeModal.close();
    });
  }
}