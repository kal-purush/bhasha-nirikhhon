import {Component, Input, forwardRef} from '@angular/core';
import {NG_VALUE_ACCESSOR, ControlValueAccessor, NG_VALIDATORS, FormControl, Validator} from '@angular/forms';

const CUSTOM_DATE_VALUE_ACCESSOR = {
    provide: NG_VALUE_ACCESSOR,
    useExisting: forwardRef(() => AuthorsComponent),
    multi: true
};

const CUSTOM_DATE_VALIDATORS = {
    provide: NG_VALIDATORS,
    useExisting: forwardRef(() => AuthorsComponent),
    multi: true
};

@Component({
    selector: 'authors',
    templateUrl: './authors.component.html',
    styleUrls: ['./authors.component.css'],
    providers: [CUSTOM_DATE_VALUE_ACCESSOR, CUSTOM_DATE_VALIDATORS]
})

export class AuthorsComponent implements ControlValueAccessor, Validator {
    parseError: boolean;
    @Input() data: any;
    authors: any = [];

    writeValue(list: [string]) {
        if (list) {
            this.authors = list;
        }
    }

    public registerOnChange(fn: any) {
        this.propagateChange = fn;
    }

    public validate(c: FormControl) {
        return (!this.parseError) ? null : {
            jsonParseError: {
                valid: false,
            }
        };
    }

    public registerOnTouched() {}

    onChange(event) {
        let newValue = event.target.value,
            isChecked = event.target.checked,
            index = this.authors.findIndex(item => item.id === +newValue),
            author = this.data.find(item => item.id === +newValue);

        if (isChecked) {
            (index < 0) && this.authors.push(author);
        } else {
            (index >= 0) && this.authors.splice(index, 1);
        }

        this.parseError = !this.authors.length;
        this.propagateChange(this.authors);
    }

    setDisabledState(isDisabled: boolean): void {}

    isChecked(id: number) {
        return this.authors.find(item => item.id === id);
    }

    private propagateChange = (_: any) => { };
}