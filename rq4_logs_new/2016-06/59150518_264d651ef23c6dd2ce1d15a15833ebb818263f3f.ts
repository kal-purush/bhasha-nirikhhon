/**
 * See https://github.com/angular/angular/issues/6895 for details.
 */
import {Directive, Provider, forwardRef, Input, ElementRef, Renderer} from '@angular/core';
import {isBlank} from '@angular/common/src/facade/lang';
import {FormBuilder, Control, FORM_PROVIDERS, NG_VALUE_ACCESSOR, ControlValueAccessor} from '@angular/common';
import {Observable} from 'rxjs/Rx';

const DEBOUNCE_INPUT_VALUE_ACCESSOR = new Provider(
    NG_VALUE_ACCESSOR, { useExisting: forwardRef(() => DebounceInputControlValueAccessor), multi: true });

@Directive({
    selector: '[debounceTime]',
    //host: {'(change)': 'doOnChange($event.target)', '(blur)': 'onTouched()'},
    providers: [DEBOUNCE_INPUT_VALUE_ACCESSOR]
})
export class DebounceInputControlValueAccessor implements ControlValueAccessor {
    onChange = (_) => { };
    onTouched = () => { };

    @Input()
    debounceTime: number;

    constructor(private _elementRef: ElementRef, private _renderer: Renderer) {

    }

    ngAfterViewInit() {
        Observable.fromEvent(this._elementRef.nativeElement, 'keyup')
            .debounceTime(this.debounceTime)
            .subscribe((event:any) => {
                this.onChange(event.target.value);
            });
    }

    writeValue(value: any): void {
        var normalizedValue = isBlank(value) ? '' : value;
        this._renderer.setElementProperty(this._elementRef.nativeElement, 'value', normalizedValue);
    }

    registerOnChange(fn: () => any): void { this.onChange = fn; }
    registerOnTouched(fn: () => any): void { this.onTouched = fn; }
}