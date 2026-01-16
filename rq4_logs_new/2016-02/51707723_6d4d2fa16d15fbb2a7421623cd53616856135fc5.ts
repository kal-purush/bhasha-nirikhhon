import {Component, OnInit}                     from 'angular2/core';

import {ControlGroup, FormBuilder, Validators} from 'angular2/common';

import {Contact}                               from './contact';

@Component({
    selector:    'new-contact',
    styleUrls:   ['app/css/newContact.css'],
    templateUrl: 'app/views/newContact.html'
})
export class NewContact implements OnInit { 
    
    myForm: ControlGroup;
    
    constructor(private _formBuilder: FormBuilder) {}
    
    ngOnInit(): any {
        this.myForm = this._formBuilder.group({
            'lastname':  ['', Validators.required],
            'firstname': ['', Validators.required],
            'age':       ['', Validators.required]
        });
    }
    
    onSubmit(value) {
        // TODO: 
    }
    
}