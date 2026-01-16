import { Component } from '@angular/core';
import { FormBuilder, FormControl, FormGroup } from '@angular/forms';

@Component({
  selector: 'add-edit-article',
  templateUrl: 'add-edit-article.component.html',
  styleUrls: ['./add-edit-article.scss']
})
export class AddEditArticleComponent {
  form: FormGroup;

  constructor(private fb: FormBuilder) {
    this.form = this.fb.group({
      link: new FormControl(''),
      date: new FormControl(''),
      keywords: new FormControl(''),
      journal: new FormControl(''),
      citation: new FormControl(''),
      title: new FormControl(''),
      abstract: new FormControl('')
    });
  }
}