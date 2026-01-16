import { Component, Input, Output, EventEmitter, SimpleChanges } from '@angular/core';
import { select } from "@angular-redux/store";
import { ActivatedRoute } from "@angular/router";
import { Observable } from "rxjs/Observable";
import { DocumentInformations, DocumentInformation } from '../information/information.types';

@Component({
    selector: "app-information-block",
    templateUrl: "./information-block.component.html",
  })
export class InformationBlockComponent {
    @Input() public informationblock: DocumentInformations;
    @Input() public maxsize: number = 3;
    public visible: Array<Object> = [];
    public invisible: Array<Object> = [];
    public show: boolean = false;

    public ngOnInit() {
      this.getArrays();
    }

    public ngOnChanges(changes: SimpleChanges){
      if(changes.hasOwnProperty("informationblock") && !!changes.informationblock.currentValue) {
        this.getArrays(changes.informationblock.currentValue);
      }
    }

    private getArrays(data: DocumentInformations = this.informationblock): void {
      this.visible = data.slice(0, this.maxsize);
      this.invisible = data.slice(this.maxsize);
    }

    public toggleVisible(){
      this.show = !this.show;
    }
}