import {Directive, ElementRef} from 'angular2/angular2';
import {HostBinding, HostListener} from 'angular2/core';

import {VisibleComponent} from './visible-component';
import {VisibleEffect} from '../effects/visible-effect';
import {ShrinkEffect} from '../effects/shrink-effect';
import {Orientation} from '../utils/alignment';

/**
 * CollapsibleComponent is similar to Collapse Plugin
 * (http://getbootstrap.com/javascript/#collapse) in Bootstrap.
 *
 * CollapsibleComponent listens for `click` events and toggles visibility of content
 * if the click target has attribute `data-toggle="collapse"`.
 */
@Directive({
	selector: '[collapse]',
	properties: ['collapse'],
	host: {
		'[class.in]': 'isExpanded',
		'[class.collapse]': 'isCollapse',
		'[class.collapsing]': 'isCollapsing',
		'[attr.aria-expanded]': 'isExpanded',
		'[attr.aria-hidden]': 'isCollapsed',
		'[style.height]': 'height'
	}
})
export class CollapsibleComponent extends VisibleComponent {

	private static effect:VisibleEffect = new ShrinkEffect(Orientation.VERTICAL);

	// @HostBinding() value:string;

	@HostListener('click', ['$event'])
	onChange(event:any) {
		console.log('event', event);
	}

	constructor(private element:ElementRef) {
		super();
	}

	expand() {

	}

	expandDone() {

	}

	collapse() {

	}

	collapseDone() {

	}
}