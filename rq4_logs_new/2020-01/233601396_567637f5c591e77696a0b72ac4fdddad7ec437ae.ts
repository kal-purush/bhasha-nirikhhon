import { Component, OnInit } from '@angular/core';
import { Note } from 'src/app/models/note.model';
import { FormGroup, FormBuilder } from '@angular/forms';
import { NotesService } from '../notes.service';

@Component({
  selector: 'app-new-note',
  templateUrl: './new-note.component.html',
  styleUrls: ['./new-note.component.scss'],
})
export class NewNoteComponent implements OnInit {
  public form: FormGroup;
  note: Note
  constructor(private formBuilder:FormBuilder, private notesService: NotesService) { }

  ngOnInit() {
    this.form = this.formBuilder.group({
      title: [''],
      text: [''],
      id: [''],
      date: ['']
    });
  }
  onSave(form){
    console.log(form.value)
    this.notesService.saveData(form.value)
  }

}