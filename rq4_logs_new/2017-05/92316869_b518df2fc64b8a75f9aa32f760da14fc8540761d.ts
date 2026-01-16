import { Injectable } from '@angular/core';
import { Popup } from './popup.model';

@Injectable()
export class PopupService {
  private popupList: Popup[] = [];
  private lastId: number = 0;

  constructor() { }

  doShow(params: any) {
    let paramsWithID: Popup = {...params, id: this.lastId};

    this.popupList.push(new Popup(paramsWithID));

    this.lastId += 1;
  }

  afterClose(popup: Popup) {
    let popupIndex: number = this.popupList.findIndex(item => item.id === popup.id);
    this.popupList.splice(popupIndex, 1);
  }

  getPopupList() {
    return this.popupList;
  }
}