/* tslint:disable: member-ordering forin */
import {Component, AfterViewChecked, ViewChild, Input} from '@angular/core';
import {NgForm, FormGroup} from '@angular/forms';
import {Address} from "./adress.model";



@Component({
    selector: 'a2d-address',
    templateUrl: './address.component.html'
})
export class AddressComponent implements AfterViewChecked {


    @Input() public address: Address;
    //@Input() public address: Address = new Address(1, '','','default');

    public submitted = false;

    public onSubmit() {
        let form = this.addressForm.form;
        this.markAllFormControlsAsDirty(form);
        this.onValueChanged(null);
        if(form.valid){
            this.submitted = true;
        }
    }

    private markAllFormControlsAsDirty(form:FormGroup) {
        //form.markAsDirty(true);
        for (const control in form.controls) {
            form.get(control).markAsDirty(true);
        }
    }

    // Reset the form with a new address AND restore 'pristine' class state
    // by toggling 'active' flag which causes the form
    // to be removed/re-added in a tick via NgIf
    // TODO: Workaround until NgForm has a reset method (#6822)
    public active = true;


    public addressForm: NgForm;
    @ViewChild('addressForm') public currentForm: NgForm;

    public ngAfterViewChecked() {
        this.formChanged();
    }

    public formChanged() {
        if (this.currentForm === this.addressForm) {
            return;
        }
        this.addressForm = this.currentForm;
        if (this.addressForm) {
            this.addressForm.valueChanges
                .subscribe(data => this.onValueChanged(data));
        }
    }

    public onValueChanged(data?: any) {
        if (!this.addressForm) {
            return;
        }
        const form = this.addressForm.form;
        for (const field in this.formErrors) {
            // clear previous error message (if any)
            this.formErrors[field] = '';
            const control = form.get(field);
            if (control && control.dirty && !control.valid) {
                const messages = this.validationMessages[field];
                for (const key in control.errors) {
                    this.formErrors[field] += messages[key] + ' ';
                }
            }
        }
    }

    public formErrors = {
        'street': '',
        'city': '',
        'state': ''
    };

    public validationMessages = {
        'street': {
            'required':      'Street is required.',
            'minlength':     'Street must be at least 4 characters long.',
            'maxlength':     'Street cannot be more than 24 characters long.'
        },
        'city': {
            'required': 'City is required.'
        },
        'state': {
            'required': 'State is required.',
            'minlength': 'State must be at least 15 characters long.',
            'maxlength': 'State cannot be more than 16 characters long.',
        }
    };
}