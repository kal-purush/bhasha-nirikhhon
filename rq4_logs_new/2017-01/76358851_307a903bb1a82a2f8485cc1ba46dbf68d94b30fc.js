import { inject } from 'aurelia-framework';
import { HttpClient, json } from 'aurelia-fetch-client';
import { DialogController } from 'aurelia-dialog';

@inject(HttpClient, DialogController)
export class EditDialog {
    constructor(httpClient, dialogController){
        this.httpClient = httpClient;
        this.dialogController = dialogController;
        this.retObj = { domain: ""};
        this.tag = "";
        this.httpClient.fetch('domains')
            .then(response => response.json())
            .then(domains => { this.domains = domains; console.log("Domains:", this.domains)})
            .catch(reason => { console.log(reason); });

    }

    activate(content) {
        this.headline = content.headline;
        this.retObj.question = content.question;
        this.retObj.tags = content.question.TagQuestions.map(tag => {return tag.TagText; });
        this.retObj.domain = content.question.DomainText;
    }

    removeTag(tagText) {
        var index = this.retObj.tags.indexOf(tagText);
        if (index > -1) {
            this.retObj.tags.splice(index, 1);
        }
    }

    addTag() {
        if(this.tag) {
            this.retObj.tags.push(this.tag);
            this.tag = "";
        }
    }

}