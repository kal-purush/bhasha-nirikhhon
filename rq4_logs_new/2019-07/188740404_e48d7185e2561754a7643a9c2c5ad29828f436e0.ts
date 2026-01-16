import { Component } from '@angular/core';
import { NavController, MenuController, NavParams, Events, AlertController, Platform } from 'ionic-angular';
import { Storage } from '@ionic/storage';
import { DatePipe } from '@angular/common';
import uuid from 'uuid/v4';

import { HttpClient } from '@angular/common/http';
import { LocationAccuracy } from '@ionic-native/location-accuracy';
import { Diagnostic } from '@ionic-native/diagnostic';
import { FormPage } from '../form/form'

@Component({
    selector: 'page-followUp',
    templateUrl: 'followUp.html'
})

export class FollowUpPage {
    template;
    selectedTemplate;
    initialForms;
    index;
    formsData;
    infoTemplates = [];
    constructor(private diagnostic: Diagnostic,
        private events: Events,
        public navParams: NavParams,
        public menuCtrl: MenuController,
        private storage: Storage,
        public navCtrl: NavController) {

        this.menuCtrl.enable(true);
        this.storage.get('infoTemplates').then((templates) => {
            this.infoTemplates = templates;
        });

        this.initialForms = this.navParams.data.forms;
        this.template = this.navParams.data.template;
        this.index = this.navParams.data.index;
        this.selectedTemplate = this.navParams.data.selectedTemplate;
        this.formsData = this.navParams.data.formsData;
    }

    increase_done_quantity() {
        for (let type of this.template.quantity) {
            if (type.type == "SEGUIMIENTO")
                type.done_quantity += 1;
        }
        this.infoTemplates[this.index] = this.template;
        this.storage.set('infoTemplates', this.infoTemplates);
    }

    decrease_done_quantity() {
        for (let type of this.template.quantity) {
            if (type.type == "SEGUIMIENTO")
                type.done_quantity -= 1;
        }
        this.infoTemplates[this.index] = this.template
        this.storage.set('infoTemplates', this.infoTemplates);
    }

    increase_remain_quantity() {
        for (let type of this.template.quantity) {
            if (type.type == "SEGUIMIENTO")
                type.remain_quantity += 1;
        }
        this.infoTemplates[this.index] = this.template
        this.storage.set('infoTemplates', this.infoTemplates);
    }

    decrease_remain_quantity() {
        for (let type of this.template.quantity) {
            if (type.type == "SEGUIMIENTO")
                type.remain_quantity -= 1;
        }
        this.infoTemplates[this.index] = this.template
        this.storage.set('infoTemplates', this.infoTemplates);
    }

    selectInterviewedClick(form) {
        // Genereate an uuid for form
        let templateUuid = this.template.uuid;
        let formUuid = uuid();
        let code_form = form.code
        let currentForm = {
            uuid: formUuid,
            code: code_form,
            name: this.template.name,
            data: {},
            type: "SEGUIMIENTO",
            createdDate: new Date()
        };
        let forms = Object.assign([], this.initialForms);
        forms.push(currentForm);
        this.formsData[templateUuid] = forms;
        this.storage.set("formsData", this.formsData);

        // this.storage.set(templateUuid, forms);
        this.increase_done_quantity();
        this.decrease_remain_quantity();
        this.navCtrl.push(FormPage, {
            template: this.template,
            selectedTemplate: this.selectedTemplate,
            formData: this.selectedTemplate,
            currentForm: currentForm,
            forms: this.initialForms,
            formsData: this.formsData
        });
    }
}