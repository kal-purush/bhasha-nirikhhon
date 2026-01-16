/// <reference path="../../typings/angular2-meteor.d.ts" />
import {Component,View} from 'angular2/angular2';
import {FORM_DIRECTIVES, FormBuilder, Control, ControlGroup, Validators} from 'angular2/angular2';
import {Pages} from 'collections/pages';
@Component({
  selector: 'pages-form'
})
@View({
  templateUrl: 'client/admin/pages-form.html',
  directives: [FORM_DIRECTIVES]
})
export class PagesForm {
  pagesForm: ControlGroup;

  constructor() {
    var fb = new FormBuilder();
    this.pagesForm = fb.group({
        name: ['',Validators.required],
        content: ['',Validators.required],
        author: ['Raj Danala']
    });
  }

  addPage(page) {
    if(this.pagesForm.valid) {
        Pages.insert({
          name: page.name,
          content: page.content,
          author: page.author
      });

      (this.pagesForm.controls['name']).updateValue('');
      (this.pagesForm.controls['content']).updateValue('');
      (this.pagesForm.controls['location']).updateValue('');
    }
  }

}