import {Component, Input, Output, ElementRef} from '@angular/core';
import {EventEmitter} from '@angular/compiler/src/facade/async';

const template = require('./editable.tpl.html') as string;

@Component({
    selector: 'warehouse-editable',
    template
})
export class EditableComponent {
    private _editMode:boolean = false;
    @Input() model;
    @Input('input-type') inputType = 'text';
    @Output() update = new EventEmitter();

    constructor(private elRef:ElementRef) {
    }
    
    onUpdateModel(newValue) {
        this.update.emit(newValue);
    }

    setEditMode(editMode = true) {
        this._editMode = editMode;
        editMode && this.focusInput();
    }

    isEditMode() {

        return this._editMode;
    }

    focusInput() {
        setTimeout(() => this.elRef.nativeElement.querySelector('input').focus())
    }
}