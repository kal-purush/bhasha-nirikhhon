import {Component, ElementRef, Renderer, HostBinding} from 'angular2/core';
import {SBItem} from './squeezebox';

@Component({
    selector: 'sb-item-body',
    template: `
        <div class="sb-item-body" [style.height]="height">
            <div class="inner"><ng-content></ng-content></div>
        </div>
    `
})
export class SBItemBody {

    private height: String = '0';
    
    constructor(private sbItem: SBItem, private el: ElementRef, private renderer: Renderer) {
        this.sbItem.body = this;
    }
    
    toggle(collapsed: boolean) {
        //this.height = 'auto';
        var height: String = '0';
        if (!collapsed) {
            var bodyEl = this.el.nativeElement.querySelector('.sb-item-body');
            bodyEl.style.height = 'auto';
            height = bodyEl.offsetHeight + 'px';
            bodyEl.style.height = '0';
        }
        setTimeout(() => this.height = height, 50);
        
    }
}