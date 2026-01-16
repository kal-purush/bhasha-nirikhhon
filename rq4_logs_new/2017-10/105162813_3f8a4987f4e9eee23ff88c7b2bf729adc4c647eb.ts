import * as $ from 'jquery';
import {noteApp} from './renderer';

export default class NoteFilterInput {
  private el: JQuery;

  constructor() {
    this.el = $('#note-filter-input');

    this.el.keypress((e: any) => {
      if (e.key === 'Enter') {
        noteApp.noteList.refresh(this.getValue());
      }
    });
  }

  public getValue(): string {
    return this.el.val() as string;
  }
}