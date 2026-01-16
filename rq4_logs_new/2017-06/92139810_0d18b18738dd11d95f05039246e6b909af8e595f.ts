import { Component, Input, Output, EventEmitter } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';
import { ModalController } from 'ionic-angular';
import { EntityProfilePage } from '../../pages/entity-profile/entity-profile';
import { SharedProvider } from '../../providers/shared.provider';
import { MongerApi } from '../../providers/api.provider';
@Component({
  selector: 'entity-card',
  templateUrl: 'entity-card.html'
})
export class EntityCardComponent {
  @Input()
  entities: any = [{}];
  @Output() unFollowEvent = new EventEmitter();
  @Output() dataUpdated = new EventEmitter();
  text: string;

  constructor(public sanitizer: DomSanitizer, public api: MongerApi, public modal: ModalController, public shared: SharedProvider) {

  }
  getBackground(image) {
    return this.sanitizer.bypassSecurityTrustStyle(`linear-gradient( rgba(29, 29, 29, 0), rgba(16, 16, 23, 0.5)), url(${image})`);
  }
  entityProfile(id, type) {
    let modal = this.modal.create(EntityProfilePage, { id: id, type });
    modal.onDidDismiss(data => {
      this.dataUpdated.next();
    })
    modal.present(); 
  }
  boomarkEntity(id, index) {
    this.entities[index].entityFollow = !this.entities[index].entityFollow;
    this.api.followEntity(id).subscribe(data => {
      this.unFollowEvent.next();
    }, err => {
      console.error(err);
    })
  }
}