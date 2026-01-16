import { Component, Output } from 'angular2/core';
import { Modal } from 'fuel-ui/fuel-ui';

@Component({
  selector: "my-modal",
  template: `
    <modal #modal
        modalTitle="Modal Title"
        [closeButton]="true"
        [closeOnUnfocus]="true" style="text-align:initial;">
        <div class="modal-body" >
            <ul>
                <li>Any</li>
                <li>Html</li>
                <li>Here</li>
            </ul>
        </div>
        <div class="modal-footer">
            <button type="button" class="btn btn-primary" (click)="modal.closeModal()">
                <i class="fa fa-chevron-left"></i> Go Back
            </button>
        </div>
    </modal>
  `,
  directives: [
    Modal
  ]
})

export class MyModalComponent {

  closeModal(){
    // modal.closeModal();
  }

  showModal(){
    // Modal.showModal();
    alert("showmodal");
    console.log(Modal);
  }
}