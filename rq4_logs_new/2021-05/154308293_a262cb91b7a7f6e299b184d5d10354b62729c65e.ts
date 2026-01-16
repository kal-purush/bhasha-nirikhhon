import { Component, Input, ChangeDetectionStrategy, ChangeDetectorRef,
  OnInit, OnDestroy, forwardRef, Optional, } from '@angular/core';
import {
  AbstractControl, ControlContainer, ControlValueAccessor,
  NgForm, NG_VALIDATORS, NG_VALUE_ACCESSOR, Validator,
} from '@angular/forms';

import { FsFile } from '@firestitch/file';

import { Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

import { get } from 'lodash-es';

import { Field } from '../../../../interfaces/field.interface';
import { FileRenderFile } from '../../../../classes/file-render-file';
import { FieldEditorService } from '../../../../services/field-editor.service';
import { controlContainerFactory } from '@firestitch/core';


@Component({
  selector: 'app-field-file-picker',
  templateUrl: 'field-file-picker.component.html',
  styleUrls: ['field-file-picker.component.scss'],
  providers: [
    {
      provide: ControlContainer,
      useFactory: controlContainerFactory,
      deps: [[new Optional(), NgForm]],
    },
    {
      provide: NG_VALUE_ACCESSOR,
      multi: true,
      useExisting: forwardRef(() => FieldFilePickerComponent),
    },
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class FieldFilePickerComponent implements OnInit, OnDestroy, ControlValueAccessor {

  @Input() public field: Field;

  public onChange = (data: any) => {};
  public onTouched = () => {};
  public allowedTypes = {};

  private _files = [];
  private _destroy$ = new Subject();

  public constructor(
    public fieldEditor: FieldEditorService,
    private _cdRef: ChangeDetectorRef,
  ) {
  }

  public selectFile(files: any) {
    this.onTouched();

    if (this.fieldEditor.config.fileUpload) {

      if (!this.field.config.configs.allowMultiple) {
        files = [files];
      }

      files.forEach((fsFile: FsFile) => {

        this.fieldEditor.config.fileUpload(this.field, fsFile.file)
        .pipe(
          takeUntil(this._destroy$)
        )
        .subscribe((response: any) => {
          const file = new FileRenderFile(response.url, response.name);
          file.value = response;

          if (!this.field.config.configs.allowMultiple) {
            this._files = [];
          }

          this._files.push(response);
          this.onChange(this._files);
          this._cdRef.markForCheck();
        });
      });
    }
  }

  public writeValue(data: any): void {
    this._files = data || [];
  }

  public registerOnChange(fn: (data: any) => void): void {
    this.onChange = fn;
  }

  public registerOnTouched(fn: () => void): void {
    this.onTouched = fn;
  }

  public ngOnInit() {

    const config = get(this.field, 'config.configs.allowedFileTypes') || {};
    const types = this._getAllowedTypes(config);

    this.allowedTypes = types.length ? types.join(',') : '*';

    const actions = [];
    if (this.fieldEditor.config && this.fieldEditor.config.fileDownload) {
      actions.push({
        label: 'Download',
        click: (item) => {
          this.fieldEditor.config.fileDownload(this.field, item);
        }
      });
    }

    if (this.fieldEditor.config && this.fieldEditor.config.fileRemove) {
      actions.push({
        label: 'Remove',
        click: (item) => {

          this.fieldEditor.config.fileRemove(this.field, item)
          .subscribe(() => {

            const idx = this._files.indexOf(item);

            if (idx >= 0) {
              this._files.splice(idx, 1);
              this.onChange(this._files);

              if (this.fieldEditor.config.fileRemoved) {
                this.fieldEditor.config.fileRemoved(this.field, item);
              }
            }
          });
        }
      });
    }
  }

  public ngOnDestroy(): void {
    this._destroy$.next();
    this._destroy$.complete();
  }

  private _getAllowedTypes(allowedTypes) {

    const allowed = [];

    if (!allowedTypes.other) {
      if (allowedTypes.image) {
        allowed.push('image/*');
      }

      if (allowedTypes.video) {
        allowed.push('video/*');
      }

      if (allowedTypes.pdf) {
        allowed.push('application/pdf');
      }
    }

    return allowed;
  }

}