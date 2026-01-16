import { Component, OnInit, OnDestroy } from "@angular/core";
import { Subscription } from "rxjs/internal/Subscription";

import { ModalService } from "./modal.service";
import { ModalComponent } from "./modal.component";

@Component({
  selector: "app-modal-container",
  template: `
    <div class="modals-container" [class.to-front]="modals.length > 0">
      <div class="dim-background" (click)="close()"></div>
      <div *ngFor="let modal of modals" class="modal-outer">
        <ng-container *ngTemplateOutlet="modal.template"></ng-container>
      </div>
    </div>
  `,
  styleUrls: ["./modal.component.scss"]
})
export class ModalContainerComponent implements OnInit, OnDestroy {
  private modalsSubscription: Subscription
  public modals: ModalComponent[] = [];

  constructor(private modalService: ModalService) { }

  ngOnInit() {
    this.modalsSubscription = this.modalService.getModals()
      .subscribe((modals: ModalComponent[]) => {
        this.modals = modals;
        console.log(modals);
      });
  }

  ngOnDestroy() {
    this.modalsSubscription.unsubscribe();
  }

  close(modal: ModalComponent) {
    this.modalService.close(modal);
  }

}