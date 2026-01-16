import { Component, Input, Output, EventEmitter, ChangeDetectionStrategy, ElementRef, Renderer2 } from '@angular/core';
import { NglAccordionSection } from './accordion-section';
import { uniqueId } from '../util/util';

@Component({
  // tslint:disable-next-line:component-selector
  selector: 'li[nglAccordionItem]',
  changeDetection: ChangeDetectionStrategy.OnPush,
  templateUrl: './accordion-item.html',
})
export class NglAccordionItem {

  @Input() isActive: boolean;

  @Input() section: NglAccordionSection;

  @Output() toggle = new EventEmitter();

  uid = uniqueId('accordion-item');

  constructor(element: ElementRef, renderer: Renderer2) {
    renderer.addClass(element.nativeElement, 'slds-accordion__list-item');
  }

  onToggle() {
    this.toggle.emit();
  }
}