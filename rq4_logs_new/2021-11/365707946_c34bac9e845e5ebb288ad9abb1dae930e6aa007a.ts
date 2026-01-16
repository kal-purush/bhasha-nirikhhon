import { EventEmitter, QueryList } from '@angular/core';
import { TextFieldRefDirective } from './text-field-ref.directive';
import { AutocompleteComponent } from '../autocomplete/autocomplete.component';
import { ErrorComponent } from '../error/error.component';

export abstract class TextFieldBaseComponent {

  public readonly focusEvent = new EventEmitter<boolean>();
  protected abstract fieldRef?: TextFieldRefDirective;
  protected abstract autoComplete?: AutocompleteComponent;
  protected abstract errors: QueryList<ErrorComponent>;

  abstract get empty(): boolean;

  protected get empty0(): boolean {
    return !this.fieldRef?.value;
  }

  abstract get error(): boolean;

  protected get error0(): boolean {
    return 0 < this.errors.length;
  }

  protected ngAfterViewInit0(): void {
    if (this.fieldRef) {
      this.fieldRef.field = this;
    }
    else {
      console.error('Could not find ', TextFieldRefDirective, ' please consider adding one.');
    }
  }

  public onKeyDown(event: KeyboardEvent): boolean {
    if (!this.autoComplete) {
      return true;
    }

    switch (event.key) {
      case 'ArrowDown':
        this.autoComplete.navigateDown();
        return false;
      case 'ArrowUp':
        this.autoComplete.navigateUp();
        return false;
      case 'Enter':
        this.autoComplete.selectCurrent();
        return false;
      default:
        return true;
    }
  }

  protected abstract onClick(): boolean;

  protected onClick0(): boolean {
    if (!this.fieldRef) {
      return false;
    }
    this.fieldRef.focus();
    return false;
  }
}