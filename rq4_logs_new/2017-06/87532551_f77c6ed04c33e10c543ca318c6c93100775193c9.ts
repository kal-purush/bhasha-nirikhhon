import { Component, Input, OnInit } from '@angular/core';
import { FormGroup } from '@angular/forms';

@Component({
    selector: 'color-picker',
    templateUrl: './color-picker.html'
})

export class ColorPicker implements OnInit {

    @Input() form: FormGroup;
    @Input() field: any = {};
    @Input() isEdit: boolean;

    color: any = null;

    constructor() {
    }

    get isValid() {
        return this.form.controls[this.field.key].valid;
    }

    ngOnInit() {
        if (this.isEdit) {
            this.form.controls[this.field.key].valueChanges
                .first()
                .subscribe(
                    value => {
                        this.onColorChange(value);
                    }
                );
        } else {
            this.onColorChange('#ffffff');
        }
    }

    onColorChange(color: any): void {
        this.color = color;
        this.form.controls[this.field.key].setValue(color);
    }
}