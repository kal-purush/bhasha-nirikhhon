import { Directive, ElementRef, HostListener, Input } from '@angular/core';

@Directive({
    selector: '[appNumberOnly]'
})
export class NumberOnlyDirective {
    @Input() isCCnumb: boolean;
    @Input() lengthRequired: number;
    constructor(private el: ElementRef) {}

    @HostListener('input', ['$event']) onInputChange(event) {
        const initalValue = this.el.nativeElement.value;
        this.el.nativeElement.value = initalValue.replace(/[^0-9]*/g, '');
        if (this.isCCnumb) {
            this.el.nativeElement.value = this.el.nativeElement.value
                .replace(/(.{4})/g, '$1 ');
        }
        this.el.nativeElement.value = this.el.nativeElement.value.substring(0, this.lengthRequired);
        if (initalValue !== this.el.nativeElement.value) {
            event.stopPropagation();
        }
    }
}