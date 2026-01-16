import {Component} from '@angular/core';
import {Directive, AfterViewChecked} from '@angular/core';

/*
 * MDLUpdateElementDirective
 *
 * update of material design lite elements, when dynamically created
 * see http://stackoverflow.com/questions/34421919/integrating-material-design-lite-with-angular2
 */

declare var componentHandler: any;

@Directive({
  selector: '[mdl]'
})

export class MDLUpdateElementDirective implements AfterViewChecked {
  ngAfterViewChecked() {
    componentHandler.upgradeAllRegistered();
  }
}