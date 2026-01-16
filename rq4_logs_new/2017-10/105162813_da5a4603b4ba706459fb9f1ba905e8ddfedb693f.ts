import * as $ from 'jquery';
import {getStatusBar} from './renderer';

export default class NoteTitleInput {
  private el: JQuery;

  constructor() {
    this.el = $('#note-title-input');

    this.el.keyup(() => {
      getStatusBar().update();
    });

    this.el.change(() => {
      getStatusBar().update();
    });
  }

  public setValue(value: string): void {
    this.el.val(value);
  }

  public getValue(): string {
    return this.el.val() as string;
  }

  public focus(): void {
    this.el.focus();
  }
}