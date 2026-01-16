import {
  Component,
  Input,
  TemplateRef,
  HostBinding,
  ChangeDetectionStrategy,
} from '@angular/core';

import { Field } from '../../../interfaces/field.interface';
import { FieldType } from '../../../enums/field-type';
import { FieldEditorService } from '../../../services/field-editor.service';


@Component({
  selector: 'fs-field-editor-item',
  templateUrl: './field-editor-item.component.html',
  styleUrls: [
    './field-editor-item.component.scss',
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class FieldEditorItemComponent {

  @Input()
  public field: Field;

  @Input()
  public fieldConfigTemplateRefs: Record<string, TemplateRef<unknown>>;

  @Input()
  public fieldRenderTemplateRefs: Record<string, TemplateRef<unknown>>;

  public fieldType = FieldType;

  constructor(
    public fieldEditor: FieldEditorService,
  ) {
  }

  @HostBinding('class.selected')
  public get isSelectedField(): boolean {
    return this.field === this.fieldEditor.selectedField;
  }

  public get fieldConfigTemplateRef(): TemplateRef<unknown> | false {
    return this.fieldConfigTemplateRefs && this.fieldConfigTemplateRefs[this.field.config.type];
  }

  public get fieldRenderTemplateRef(): TemplateRef<unknown> | false {
    return this.fieldRenderTemplateRefs && this.fieldRenderTemplateRefs[this.field.config.type];
  }

  public get showRequired(): boolean {
    return this.field.config.configs.showRequired !== false
      && this.field.config.type !== FieldType.Address
      && this.field.config.type !== FieldType.Heading
      && this.field.config.type !== FieldType.Content
      && this.field.config.type !== FieldType.File
  }

  public get showDescription(): boolean {
    return this.field.config.configs.showDescription !== false
      && this.field.config.type !== FieldType.Heading
      && this.field.config.type !== FieldType.Content
  }
}