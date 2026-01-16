import {Component, OnInit} from 'angular2/core';

import {NgForm}            from 'angular2/common';

import {Contact}           from './contact';

@Component({
    selector:    'new-contact-form',
    styleUrls:   ['app/css/newContactForm.css', 'app/css/common.css'],
    templateUrl: 'app/views/newContactForm.html'
})
export class NewContactForm implements OnInit { 
    
    model: Contact;
    
    active = true;
    
    constructor() {}
    
    ngOnInit(): any {
        this.model = new Contact('', '');
    }
    
    onSubmit() {
        // TODO: 
    }
    
    newContact() {
        // TODO:
        this.model = new Contact('', '');
        this.active = false;
        setTimeout(() => this.active = true, 0); // refresh form
    }
    
}