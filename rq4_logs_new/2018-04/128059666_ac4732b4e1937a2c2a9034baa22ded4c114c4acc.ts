import { Component, Input, Output, EventEmitter } from '@angular/core';
import { select } from "@angular-redux/store";
import { ActivatedRoute } from "@angular/router";
import { Observable } from "rxjs/Observable";

@Component({
    selector: "app-destination",
    templateUrl: "./destination.component.html",
  })
export class DestinationComponent {
    @Input() public destination;

    @Output() public clicked = new EventEmitter();

    public submit() {
      this.clicked.emit(this.destination);
    }
}