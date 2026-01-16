import { Component, Input } from '@angular/core'
import { MD_LIST_DIRECTIVES } from '@angular2-material/list'
import { MdIcon, MdIconRegistry } from '@angular2-material/icon'
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button'
import { ISchema } from "../../shared/schema/parsers/v3.parser"

@Component({
  moduleId: module.id,
  selector: 'app-schema-item',
  templateUrl: 'schema-item.component.html',
  directives: [MD_LIST_DIRECTIVES, MdIcon, MD_BUTTON_DIRECTIVES],
  providers: [MdIconRegistry]
})
export class SchemaItemComponent {
  @Input() schema: ISchema

  public isExpanded: boolean

  public constructor() {
    this.isExpanded = false
  }

  public toggleExpand(): void {
    this.isExpanded = !this.isExpanded
  }

}